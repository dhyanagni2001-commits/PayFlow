"""
Sanity-check the landing zone and print Phase 1 metrics.

Run:  python scripts/check_landing.py

Answers four questions:
  1. Did every table's events arrive, and what kinds (insert/update/delete)?
  2. Any duplicate events? (expected: 0 in normal runs; >0 only after a crash,
     which is fine because silver dedupes on the Kafka offset)
  3. How fast? Latency = time we wrote the file minus time Postgres committed
     the change. This is your first resume number.
  4. Are files a healthy size, and did anything land in the dead-letter file?

WHY DUCKDB: it queries a folder of Parquet files with plain SQL, in-process, no
server, free. Same SQL you'd write in Databricks later.

Edge cases handled: no files yet (clear message, exit 1), files only in the
archive (already uploaded), missing dead-letter file (= 0).
"""

import os
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
LANDING = Path(os.getenv("LANDING_DIR", ROOT / "landing"))
ARCHIVE = Path(os.getenv("ARCHIVE_DIR", ROOT / "landing_archive"))   # files the uploader already sent


def parquet_sources() -> str:
    """DuckDB list literal of globs for every folder that actually has files."""
    dirs = [d for d in (LANDING, ARCHIVE) if any(d.glob("*/*/*.parquet"))]
    if not dirs:
        raise SystemExit(f"No Parquet files under {LANDING} or {ARCHIVE}. Is the consumer running?")
    return "[" + ",".join(f"'{d}/*/*/*.parquet'" for d in dirs) + "]"


def main() -> None:
    files = parquet_sources()
    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet({files})")

    # 1. Volume per table and operation.
    print("\n== 1. Events by table and operation (c=insert, u=update, d=delete, r=snapshot) ==")
    print(con.sql("""
        SELECT table_name, op, COUNT(*) AS events
        FROM events GROUP BY 1, 2 ORDER BY 1, 2
    """))

    # 2. Duplicates by event id.
    print("\n== 2. Duplicate check (same Kafka topic/partition/offset landed twice) ==")
    print(con.sql("""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT (_kafka_topic, _kafka_partition, _kafka_offset)) AS unique_events,
               COUNT(*) - COUNT(DISTINCT (_kafka_topic, _kafka_partition, _kafka_offset)) AS duplicates
        FROM events
    """))

    # 3. Latency. Snapshot reads (op='r') are excluded: their source_ts_ms is
    #    the snapshot time, not a real change, so they'd distort latency.
    #    This latency includes the consumer's flush wait. With --flush-seconds
    #    30, expect p50 around 15s. Lower flush = lower latency = more small
    #    files. That's the tradeoff to talk about.
    print("\n== 3. Latency: Postgres commit -> Parquet file on disk (seconds) ==")
    print(con.sql("""
        WITH l AS (
            SELECT (epoch_ms(_ingested_at) - source_ts_ms) / 1000.0 AS sec
            FROM events WHERE op <> 'r' AND source_ts_ms IS NOT NULL AND _ingested_at IS NOT NULL
        )
        SELECT COUNT(*) AS events,
               ROUND(quantile_cont(sec, 0.50), 2) AS p50_s,
               ROUND(quantile_cont(sec, 0.95), 2) AS p95_s,
               ROUND(quantile_cont(sec, 0.99), 2) AS p99_s,
               ROUND(MAX(sec), 2)                 AS max_s
        FROM l
    """))

    # 4. File sizes + dead letters.
    print("\n== 4. Files written (watch for too many tiny files) ==")
    print(con.sql(f"""
        SELECT COUNT(DISTINCT filename) AS files,
               ROUND(COUNT(*) / COUNT(DISTINCT filename), 0) AS avg_events_per_file
        FROM read_parquet({files}, filename = true)
    """))

    dlq = LANDING / "_dead_letter" / "events.jsonl"
    n_dlq = sum(1 for _ in dlq.open()) if dlq.exists() else 0
    print(f"\nDead-letter events: {n_dlq}")


if __name__ == "__main__":
    main()
