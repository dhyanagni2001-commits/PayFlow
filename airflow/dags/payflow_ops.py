"""
Operational DAGs:

  payflow_daily_reconciliation  00:37 UTC daily. Source vs lakehouse, to the cent.
  payflow_maintenance           weekly. OPTIMIZE + VACUUM, records file counts.
  payflow_replay                manual. Rebuild silver/gold from bronze.

Kept in one file because they share helpers and are small.

  1. payflow_daily_reconciliation
  2. payflow_maintenance
  3. payflow_replay

Edge cases handled: a maintained table that doesn't exist yet is skipped (not
fatal), and the daily check records its result BEFORE failing, so Tableau
shows failed reconciliations too.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {"owner": "payflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
CATALOG = os.getenv("PAYFLOW_CATALOG") or "payflow"


# -----------------------------------------------------------------------------
# 1. Daily reconciliation
# WHY 00:37 and not 00:00: the day just closed; give the 30-min pipeline one
# run to catch up on the last events of the day. Odd minute = not competing
# with every other job scheduled at :00.
# -----------------------------------------------------------------------------
@dag(dag_id="payflow_daily_reconciliation", schedule="37 0 * * *", start_date=datetime(2026, 10, 1),
     catchup=False, default_args=default_args, tags=["payflow", "quality"])
def payflow_daily_reconciliation():

    @task
    def reconcile_closed_days():
        from reconciliation.reconcile import record, run
        results = run("daily", days=3)
        record("daily", results)
        bad = [r for r in results if not r["match"]]
        print(f"{len(results)} checks, {len(bad)} mismatches")
        if bad:
            # Task failure = alert (email/Slack callback in production).
            raise RuntimeError(f"Reconciliation mismatches: {bad[:5]}")

    reconcile_closed_days()


# -----------------------------------------------------------------------------
# 2. Weekly maintenance
# WHY: frequent small micro-batches create many small files ("small file
# problem"), and every query pays to open them. OPTIMIZE compacts them; VACUUM
# deletes files no longer referenced (after the default 7-day retention, which
# keeps time travel working for a week).
# Databricks may also run predictive optimization on managed tables. We still
# schedule it to make the behavior explicit and to MEASURE it.
# -----------------------------------------------------------------------------
MAINTAINED = ["bronze.cdc_events", "silver.payments_history", "silver.payments_state",
              "silver.refunds_history", "silver.merchants_history"]


@dag(dag_id="payflow_maintenance", schedule="13 3 * * 0", start_date=datetime(2026, 10, 1),
     catchup=False, default_args=default_args, tags=["payflow", "ops"])
def payflow_maintenance():

    @task
    def optimize_and_vacuum():
        from payflow_common.connections import dbx_query, dbx_query_dicts
        for t in MAINTAINED:
            name = f"{CATALOG}.{t}"
            try:
                files_before = dbx_query_dicts(f"DESCRIBE DETAIL {name}")[0]["numFiles"]
            except Exception as e:
                if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                    print(f"{t}: not created yet, skipped")
                    continue
                raise
            dbx_query(f"OPTIMIZE {name}")
            dbx_query(f"VACUUM {name}")
            after = dbx_query_dicts(f"DESCRIBE DETAIL {name}")[0]
            files_after, size = after["numFiles"], after["sizeInBytes"]
            dbx_query(f"INSERT INTO {CATALOG}.ops.maintenance_runs VALUES "
                      f"(current_timestamp(), '{t}', {files_before}, {files_after}, {size})")
            print(f"{t}: {files_before} -> {files_after} files")

    optimize_and_vacuum()


# -----------------------------------------------------------------------------
# 3. Replay
# WHY: bronze is immutable, so any silver/gold bug is fixed by deploying the
# corrected code and replaying. Also used to prove determinism: replaying the
# same bronze must produce identical gold totals.
# -----------------------------------------------------------------------------
@dag(dag_id="payflow_replay", schedule=None, start_date=datetime(2026, 10, 1),
     catchup=False, default_args=default_args, tags=["payflow", "ops"])
def payflow_replay():
    DatabricksRunNowOperator(
        task_id="full_refresh_from_bronze",
        databricks_conn_id="databricks_default",
        job_name="payflow-lakehouse",
        job_parameters={"catalog": CATALOG, "full_refresh": "true"},
        wait_for_termination=True,
    )


payflow_daily_reconciliation()
payflow_maintenance()
payflow_replay()
