"""
Export the gold and ops tables from Databricks to CSV files for Tableau Public.

Run:  python scripts/export_for_tableau.py        (or: make tableau-export)

WHY CSV (not a live Databricks connection):
    Tableau Public, the free edition that can publish to the web, only reads
    files. It has no Databricks connector. Published workbooks embed an extract
    anyway, so a CSV snapshot loses nothing. Re-run this script to refresh.

WHY RESHAPE HERE (not in Tableau):
    Every dashboard needs dollars, not cents, and merchant names, not ids.
    Doing it once in SQL keeps the Tableau side to drag-and-drop, and the SQL
    is reviewable in git while a .twb file is not.

Files written to tableau/data/:
    settlement.csv          gold.daily_merchant_settlement + merchant name/category
    volume_10min.csv       gold.fct_payment_lifecycle in 10-minute buckets (the trend
                            line: the simulated history spans a few hours, so a
                            daily chart would be a single point)
    merchant_risk.csv       gold.merchant_risk_30d
    dq_flags.csv            ops.dq_results (one row per flagged record)
    dq_catch_rate.csv       ops.dq_catch_rate_history
    pipeline_runs.csv       ops.pipeline_runs (+ is_backfill for the initial load)
    reconciliation_runs.csv ops.reconciliation_runs (without the free-text details)
    maintenance_runs.csv    ops.maintenance_runs (empty until payflow_maintenance runs)

Edge cases handled: empty tables still produce a CSV with headers, so Tableau
data sources don't break before the first maintenance run; timestamps are
written as naive UTC ("2026-10-06 13:21:31") because Tableau doesn't parse
the "+00:00" offset Python adds.
"""

from __future__ import annotations

import csv
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from payflow_common.connections import catalog, check_dbx_env, dbx_sql_connect  # noqa: E402

OUT = ROOT / "tableau" / "data"

# Runs whose freshness is over an hour are the one-off backfill of history, not steady state.
BACKFILL_SECONDS = 3600


def queries(c: str) -> dict[str, str]:
    return {
        "settlement": f"""
            SELECT s.settlement_date, s.merchant_id, m.name AS merchant_name, m.category, s.currency,
                   s.captured_count,
                   s.gross_cents / 100.0 AS gross, s.fee_cents / 100.0 AS fees,
                   s.refund_cents / 100.0 AS refunds, s.dispute_lost_cents / 100.0 AS chargebacks,
                   s.net_cents / 100.0 AS net, s.running_balance_cents / 100.0 AS running_balance
            FROM {c}.gold.daily_merchant_settlement s
            LEFT JOIN {c}.gold.dim_merchant_scd2 m ON m.merchant_id = s.merchant_id AND m.is_current
            ORDER BY s.settlement_date, s.merchant_id, s.currency""",
        "volume_10min": f"""
            SELECT timestamp_seconds(floor(unix_timestamp(created_at) / 600) * 600) AS time_10min, currency,
                   COUNT(*) AS payments,
                   SUM(CASE WHEN captured_at IS NOT NULL THEN amount_cents ELSE 0 END) / 100.0 AS captured,
                   SUM(refunded_cents) / 100.0 AS refunded,
                   SUM(CASE WHEN dispute_lost THEN amount_cents ELSE 0 END) / 100.0 AS chargebacks,
                   SUM(CASE WHEN current_status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM {c}.gold.fct_payment_lifecycle
            WHERE NOT is_flagged
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "merchant_risk": f"""
            SELECT merchant_id, name AS merchant_name, category, current_risk_tier,
                   captured_30d, disputes_30d, disputes_lost_30d, refunded_30d, flagged_30d,
                   chargeback_rate, refund_rate
            FROM {c}.gold.merchant_risk_30d""",
        "dq_flags": f"""
            SELECT rule_name, table_name, pk, observed, first_detected_at
            FROM {c}.ops.dq_results""",
        "dq_catch_rate": f"""
            SELECT run_ts, injected, caught, false_positives, flagged_total,
                   caught / NULLIF(injected, 0) AS catch_rate
            FROM {c}.ops.dq_catch_rate_history ORDER BY run_ts""",
        "pipeline_runs": f"""
            SELECT run_ts, events_processed, bronze_rows, max_source_ts,
                   freshness_seconds / 60.0 AS freshness_min,
                   latency_p50_s / 60.0 AS latency_p50_min, latency_p95_s / 60.0 AS latency_p95_min,
                   latency_p99_s / 60.0 AS latency_p99_min,
                   freshness_seconds > {BACKFILL_SECONDS} AS is_backfill
            FROM {c}.ops.pipeline_runs ORDER BY run_ts""",
        "reconciliation_runs": f"""
            SELECT run_ts, mode, checks, mismatches
            FROM {c}.ops.reconciliation_runs ORDER BY run_ts""",
        "maintenance_runs": f"""
            SELECT run_ts, table_name, files_before, files_after, size_bytes / 1048576.0 AS size_mb
            FROM {c}.ops.maintenance_runs ORDER BY run_ts""",
    }


def cell(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    return v


def main() -> None:
    check_dbx_env()
    OUT.mkdir(parents=True, exist_ok=True)
    with dbx_sql_connect() as conn, conn.cursor() as cur:
        for name, sql_text in queries(catalog()).items():
            cur.execute(sql_text)
            header = [d[0] for d in cur.description]
            rows = cur.fetchall()
            with open(OUT / f"{name}.csv", "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(header)
                w.writerows([cell(v) for v in r] for r in rows)
            print(f"{name + '.csv':26} {len(rows):>7,} rows")
    print(f"written to {OUT}")


if __name__ == "__main__":
    main()
