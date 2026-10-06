"""
Reconcile the source Postgres against the lakehouse silver layer, to the cent.

Run:
    python reconciliation/reconcile.py --mode daily --days 3
    python reconciliation/reconcile.py --mode full     # only after stopping the simulator

TWO MODES, because CDC is always slightly behind the source:

  daily  Runs while traffic flows (Airflow, every night). Compares only facts
         that never change after insert (row count and SUM(amount_cents) per
         created day) for CLOSED days (before today, UTC). Rows inserted today
         are excluded, so pipeline lag can't cause false alarms. Expect an exact
         match, every day.

  full   Quiesced check (simulator stopped, pipeline drained). Compares
         everything including mutable state: rows per status per table, and
         customer count (proves deletes propagated). Used after chaos tests.

WHY NOT A ROW-BY-ROW HASH COMPARE: Postgres and Spark hash differently, and
pulling every row out of both systems doesn't scale. Grouped counts and sums
in integer cents catch missing rows, duplicates and wrong amounts at a tiny
cost. The tradeoff: two errors that cancel out exactly (one row missing, one
duplicated with the same amount) would slip through. Status-level counts in
full mode narrow that further.

Exit code 1 on any mismatch, so Airflow marks the task failed and alerts.

Steps in run():
    1. build the paired (Postgres, Databricks) queries for the mode
    2. run them on both sides (Postgres session pinned to UTC)
    3. compare() every (table, key): count AND sum of cents must be equal
    4. record() the run in ops.reconciliation_runs (details bound as a parameter)

Edge cases handled:
    - a day/status present on only one side -> compared against (0, 0), so a
      whole missing day is a mismatch, not silently ignored
    - NULL sums (no rows) -> COALESCE to 0 on both sides
    - Decimal vs int return types between drivers -> normalized with int()
    - --days < 1 rejected
    - a silver table that doesn't exist yet -> reported as a mismatch for that
      table instead of crashing the whole reconciliation
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from payflow_common.connections import catalog, dbx_query, pg_connect  # noqa: E402

AMOUNT_TABLES = ["payments", "refunds", "disputes"]


def daily_queries(start: date, end: date) -> tuple[dict[str, str], dict[str, str]]:
    """(postgres_sql, databricks_sql) per table: day, count, sum of cents."""
    pg, dbx = {}, {}
    for t in AMOUNT_TABLES:
        pg[t] = (f"SELECT (created_at AT TIME ZONE 'UTC')::date::text, COUNT(*), COALESCE(SUM(amount_cents),0) "
                 f"FROM {t} WHERE created_at >= '{start}' AND created_at < '{end}' GROUP BY 1")
        dbx[t] = (f"SELECT CAST(TO_DATE(created_at) AS STRING), COUNT(*), COALESCE(SUM(amount_cents),0) "
                  f"FROM {catalog()}.silver.{t} WHERE created_at >= '{start}' AND created_at < '{end}' GROUP BY 1")
    return pg, dbx


def full_queries() -> tuple[dict[str, str], dict[str, str]]:
    """Status-level counts/sums for every table, plus customers (deletes)."""
    pg, dbx = {}, {}
    for t in AMOUNT_TABLES:
        pg[t] = f"SELECT status, COUNT(*), COALESCE(SUM(amount_cents),0) FROM {t} GROUP BY 1"
        dbx[t] = f"SELECT status, COUNT(*), COALESCE(SUM(amount_cents),0) FROM {catalog()}.silver.{t} GROUP BY 1"
    pg["merchants"] = "SELECT risk_tier, COUNT(*), COALESCE(SUM(fee_bps),0) FROM merchants GROUP BY 1"
    dbx["merchants"] = f"SELECT risk_tier, COUNT(*), COALESCE(SUM(fee_bps),0) FROM {catalog()}.silver.merchants GROUP BY 1"
    pg["customers"] = "SELECT 'all', COUNT(*), 0 FROM customers"
    dbx["customers"] = f"SELECT 'all', COUNT(*), 0 FROM {catalog()}.silver.customers"
    return pg, dbx


def compare(source: dict[str, list[tuple]], lake: dict[str, list[tuple]]) -> list[dict]:
    """
    Pure comparison (unit tested). Rows are (key, count, sum).
    Returns one result per (table, key) present on either side.
    """
    results = []
    for table in sorted(set(source) | set(lake)):
        src = {str(r[0]): (int(r[1]), int(r[2])) for r in source.get(table, [])}
        lak = {str(r[0]): (int(r[1]), int(r[2])) for r in lake.get(table, [])}
        for key in sorted(set(src) | set(lak)):
            sv, lv = src.get(key, (0, 0)), lak.get(key, (0, 0))
            results.append({
                "table": table, "key": key,
                "source_count": sv[0], "lake_count": lv[0],
                "source_cents": sv[1], "lake_cents": lv[1],
                "match": sv == lv,
            })
    return results


def run(mode: str, days: int) -> list[dict]:
    # 1. Queries for the mode.
    if mode == "daily":
        end = datetime.now(timezone.utc).date()          # today is still open: excluded
        pg_sql, dbx_sql = daily_queries(end - timedelta(days=days), end)
    else:
        pg_sql, dbx_sql = full_queries()

    # 2. Source side.
    with pg_connect() as conn, conn.cursor() as cur:
        source = {}
        for t, q in pg_sql.items():
            cur.execute(q)
            source[t] = cur.fetchall()
    # 2b. Lakehouse side.
    lake = {}
    for t, q in dbx_sql.items():
        try:
            lake[t] = dbx_query(q)
        except Exception as e:  # table missing / not built yet
            if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
                raise
            print(f"  silver.{t} not found; every source row counts as missing")
            lake[t] = []
    # 3. Compare to the cent.
    return compare(source, lake)


def record(mode: str, results: list[dict]) -> None:
    """Store the run in ops.reconciliation_runs so Tableau can chart it."""
    mismatches = [r for r in results if not r["match"]]
    dbx_query(
        f"INSERT INTO {catalog()}.ops.reconciliation_runs VALUES "
        f"(current_timestamp(), :mode, :checks, :mismatches, :details)",
        {"mode": mode, "checks": len(results), "mismatches": len(mismatches),
         "details": json.dumps(mismatches[:50], default=str)},
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["daily", "full"], default="daily")
    p.add_argument("--days", type=int, default=3, help="closed days to check (daily mode)")
    args = p.parse_args()
    if args.days < 1:
        p.error("--days must be >= 1")

    results = run(args.mode, args.days)
    record(args.mode, results)
    bad = [r for r in results if not r["match"]]
    cents = sum(r["source_cents"] for r in results if r["table"] == "payments")
    print(f"{args.mode}: {len(results)} checks, {len(bad)} mismatches, "
          f"${cents / 100:,.2f} of payments compared")
    for r in bad:
        print(f"  MISMATCH {r}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
