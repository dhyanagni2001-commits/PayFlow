"""
Prove the CDC path lost nothing: compare Postgres (truth) with the landing
files (what the pipeline captured), row by row.

Run AFTER stopping the simulator and letting the consumer drain:
    python scripts/verify_no_loss.py

For every table:
  1. every primary key in Postgres must have a latest landed event that is not a delete
  2. every key whose latest landed event is a delete must be gone from Postgres
  3. the latest landed "state" must equal Postgres (proves UPDATES arrived in
     order, not just inserts):
       payments / refunds / disputes -> status
       merchants                     -> risk_tier|fee_bps  (drives SCD2)
       customers                     -> existence only

Exit code 1 on any difference. Used by CI and by every chaos scenario.

WHY CHECK LANDING (not silver): this isolates the capture half of the system
(Postgres -> Debezium -> Kafka -> consumer). Silver is checked separately by
reconcile.py --mode full. When something breaks, you know which half.

Edge cases handled: a table with no landed files (all rows reported missing),
duplicates in landing (latest-per-key by LSN, then offset), Postgres down
(clear message, exit 2).
"""

import os
import sys
from pathlib import Path

import duckdb
import psycopg

ROOT = Path(__file__).resolve().parent.parent
LANDING = Path(os.getenv("LANDING_DIR", ROOT / "landing"))
ARCHIVE = Path(os.getenv("ARCHIVE_DIR", ROOT / "landing_archive"))
PG_DSN = os.getenv("PG_DSN", "postgresql://payflow:payflow@localhost:5432/payflow")
PKS = {"merchants": "merchant_id", "customers": "customer_id", "payments": "payment_id",
       "refunds": "refund_id", "disputes": "dispute_id"}

# What "state" to compare per table: (Postgres SQL expression, DuckDB expression over `after`).
STATE = {
    "payments":  ("status", "json_extract_string(after, '$.status')"),
    "refunds":   ("status", "json_extract_string(after, '$.status')"),
    "disputes":  ("status", "json_extract_string(after, '$.status')"),
    "merchants": ("risk_tier || '|' || fee_bps",
                  "json_extract_string(after, '$.risk_tier') || '|' || json_extract_string(after, '$.fee_bps')"),
    "customers": ("NULL", "NULL"),
}


def latest_events(con, table: str, pk: str):
    """Latest landed event per key -> {pk: (op, state)}."""
    globs = [str(d / table / "*" / "*.parquet") for d in (LANDING, ARCHIVE) if any((d / table).glob("*/*.parquet"))]
    if not globs:
        return {}
    files = "[" + ",".join(f"'{g}'" for g in globs) + "]"
    rows = con.sql(f"""
        WITH e AS (
            SELECT CAST(json_extract(primary_key, '$.{pk}') AS BIGINT) AS id, op,
                   {STATE[table][1]} AS state, source_lsn, _kafka_offset
            FROM read_parquet({files})
        )
        SELECT id, op, state FROM e
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY source_lsn DESC, _kafka_offset DESC) = 1
    """).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def diff_table(pg_rows: dict, landed: dict, check_status: bool) -> dict:
    """Pure comparison (unit tested). pg_rows: {pk: state}, landed: {pk: (op, state)}."""
    alive = {k for k, (op, _) in landed.items() if op != "d"}
    missing = sorted(set(pg_rows) - alive)                  # in Postgres, not captured
    extra = sorted(alive - set(pg_rows))                    # captured as alive, but gone in Postgres (missed delete)
    wrong = sorted(k for k in set(pg_rows) & alive
                   if check_status and pg_rows[k] != landed[k][1])
    return {"pg": len(pg_rows), "landed_alive": len(alive), "missing": missing[:10], "n_missing": len(missing),
            "extra": extra[:10], "n_extra": len(extra), "wrong_status": wrong[:10], "n_wrong": len(wrong)}


def main() -> None:
    con = duckdb.connect()
    failed = False
    try:
        pg = psycopg.connect(PG_DSN, connect_timeout=5)
    except psycopg.OperationalError as e:
        print(f"Cannot reach Postgres at {PG_DSN}: {e}")
        sys.exit(2)
    with pg, pg.cursor() as cur:
        for table, pk in PKS.items():
            pg_expr = STATE[table][0]
            check = pg_expr != "NULL"
            cur.execute(f"SELECT {pk}, {pg_expr} FROM {table}")
            pg_rows = {r[0]: r[1] for r in cur.fetchall()}
            d = diff_table(pg_rows, latest_events(con, table, pk), check_status=check)
            ok = d["n_missing"] == d["n_extra"] == d["n_wrong"] == 0
            failed |= not ok
            print(f"{'OK  ' if ok else 'FAIL'} {table:10s} postgres={d['pg']:>7} landed={d['landed_alive']:>7} "
                  f"missing={d['n_missing']} extra={d['n_extra']} wrong_state={d['n_wrong']}")
            if not ok:
                print(f"      samples: missing={d['missing']} extra={d['extra']} wrong={d['wrong_status']}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
