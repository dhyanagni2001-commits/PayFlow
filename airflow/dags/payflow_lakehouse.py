"""
Main pipeline DAG, every hour:

  1. check_replication_slot -> 2. upload_landing_files -> [skip if nothing new]
      -> 3. run_databricks_job -> 4. check_freshness (runs even when 3 is skipped)

WHY AIRFLOW ORCHESTRATES BUT DOESN'T PROCESS:
    Airflow decides WHEN and IN WHAT ORDER; Databricks does the data work.
    Heavy processing inside Airflow workers is a classic anti-pattern: the
    scheduler gets starved and retries become expensive.

WHY HOURLY (not every 1 min, and not every 30 min):
    Each run spins up serverless compute, and Free Edition has a daily compute
    quota. Measured: at every 30 min (48 runs/day, ~7-8 min each) the quota ran
    out around 06:00 UTC and Databricks refused new runs ("Triggering new runs
    ... is currently disabled temporarily") until ~12:00 UTC. Nothing was lost
    (files kept uploading, the next allowed run caught up), but freshness had a
    6-hour hole. Hourly halves the compute and keeps p95 freshness around an
    hour. With a paid workspace, drop this to minutes or stream from Kafka.

WHY SHORT-CIRCUIT when nothing was uploaded:
    No new files = nothing to process. Skipping the Databricks run saves quota.

Edge cases handled:
    - replication slot MISSING (connector deleted, DB restored) fails loudly;
      the naive query would return no row and report "lag 0"
    - freshness is checked even when the job was skipped: "no new files" is
      exactly what a dead consumer looks like, so that's when it matters most
    - freshness = newest change in POSTGRES minus newest change in SILVER, so
      an idle source (simulator stopped) doesn't raise a false alarm
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Alert thresholds. Tune from measurements, don't guess.
SLOT_LAG_FAIL_BYTES = 512 * 1024 * 1024   # Debezium falling behind: WAL piling up on the source
FRESHNESS_FAIL_SECONDS = 2 * 60 * 60       # silver more than 2h behind the source = something's stuck
SLOT_NAME = "payflow_slot"

default_args = {
    "owner": "payflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


def freshness_lag_seconds(source_max: datetime | None, silver_max: datetime | None) -> float:
    """
    Pure helper (unit tested). How far silver trails the source, in seconds.
    Naive datetimes are treated as UTC (the Databricks driver returns naive UTC).
    """
    def utc(d):
        return d if d is None or d.tzinfo else d.replace(tzinfo=timezone.utc)

    source_max, silver_max = utc(source_max), utc(silver_max)
    if source_max is None:
        return 0.0                       # empty source: nothing to be behind on
    if silver_max is None:
        return float("inf")              # source has data, lakehouse has never seen any
    return max(0.0, (source_max - silver_max).total_seconds())


@dag(
    dag_id="payflow_lakehouse",
    schedule="0 * * * *",       # hourly: see "WHY HOURLY" above
    start_date=datetime(2026, 10, 1),
    catchup=False,            # don't backfill missed intervals: the data is in Kafka/landing anyway
    max_active_runs=1,        # never two uploads/jobs at once (silver watermark relies on order)
    default_args=default_args,
    tags=["payflow"],
)
def payflow_lakehouse():

    @task
    def check_replication_slot() -> int:
        """
        1. How much WAL is Postgres holding for Debezium?
        If Debezium stops reading, Postgres keeps every WAL segment for the slot
        and can eventually fill its disk and take the payments DB down. This is
        the #1 operational risk of log-based CDC, so we check it every run.
        """
        from payflow_common.connections import pg_connect
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint, active
                FROM pg_replication_slots WHERE slot_name = %s
            """, (SLOT_NAME,))
            row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Replication slot {SLOT_NAME} does not exist. Run `make register`.")
        lag, active = int(row[0]), row[1]
        print(f"replication slot lag: {lag / 1e6:.2f} MB, active={active}")
        if lag > SLOT_LAG_FAIL_BYTES:
            raise RuntimeError(f"Replication slot lag {lag / 1e6:.0f} MB > threshold. Is Debezium running?")
        return lag

    # ignore_downstream_trigger_rules=False: only the NEXT task (the job) is
    # skipped; check_freshness still runs because of its trigger rule.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def upload_landing_files() -> bool:
        """2. Push new landing files to the Volume. False -> skip the Databricks job."""
        from uploader.upload_to_volume import upload_all
        return upload_all() > 0

    # 3. Run the lakehouse job (bronze -> silver -> quality -> gold).
    run_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_name="payflow-lakehouse",     # resolved to a job id at runtime (no hard-coded ids)
        job_parameters={"catalog": os.getenv("PAYFLOW_CATALOG") or "payflow", "full_refresh": "false"},
        wait_for_termination=True,
    )

    @task(trigger_rule="none_failed")
    def check_freshness() -> float:
        """4. Fail loudly if silver trails the source by more than the threshold."""
        from payflow_common.connections import catalog, dbx_query, pg_connect
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT GREATEST((SELECT MAX(updated_at) FROM payments), (SELECT MAX(updated_at) FROM refunds),"
                        " (SELECT MAX(updated_at) FROM disputes), (SELECT MAX(updated_at) FROM merchants),"
                        " (SELECT MAX(updated_at) FROM customers))")
            source_max = cur.fetchone()[0]
        rows = dbx_query(f"SELECT max_source_ts, latency_p95_s FROM {catalog()}.ops.pipeline_runs "
                         f"ORDER BY run_ts DESC LIMIT 1")
        silver_max, p95 = rows[0] if rows else (None, None)
        lag = freshness_lag_seconds(source_max, silver_max)
        print(f"source_max={source_max} silver_max={silver_max} lag={lag}s latency_p95={p95}s")
        if lag > FRESHNESS_FAIL_SECONDS:
            raise RuntimeError(f"Silver is {lag:.0f}s behind the source")
        return lag

    check_replication_slot() >> upload_landing_files() >> run_job >> check_freshness()


payflow_lakehouse()
