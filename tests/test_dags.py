"""
DAG integrity test: every DAG file imports cleanly, has the expected tasks,
and no DAG has cycles. Catches typos before they reach the scheduler, where
a broken DAG file silently disappears from the UI.

Run:  python -m pytest tests/test_dags.py -v   (needs apache-airflow installed; skipped otherwise)
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parent.parent / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_and_tasks(dagbag):
    assert set(dagbag.dag_ids) == {
        "payflow_lakehouse", "payflow_daily_reconciliation", "payflow_maintenance", "payflow_replay",
    }
    main = dagbag.get_dag("payflow_lakehouse")
    assert [t.task_id for t in main.topological_sort()] == [
        "check_replication_slot", "upload_landing_files", "run_databricks_job", "check_freshness",
    ]
    assert main.max_active_runs == 1 and main.catchup is False


def test_replay_passes_full_refresh(dagbag):
    op = dagbag.get_dag("payflow_replay").get_task("full_refresh_from_bronze")
    assert op.json["job_parameters"]["full_refresh"] == "true"


def test_freshness_runs_even_when_job_is_skipped(dagbag):
    """A dead consumer = no files = job skipped. Freshness must still be checked."""
    main = dagbag.get_dag("payflow_lakehouse")
    assert main.get_task("check_freshness").trigger_rule == "none_failed"
    assert main.get_task("upload_landing_files").ignore_downstream_trigger_rules is False


def test_freshness_lag_helper():
    import sys
    from datetime import datetime, timedelta, timezone
    sys.path.insert(0, str(DAGS))
    from payflow_lakehouse import freshness_lag_seconds
    now = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
    assert freshness_lag_seconds(None, None) == 0.0                      # empty source
    assert freshness_lag_seconds(now, None) == float("inf")              # never loaded
    assert freshness_lag_seconds(now, (now - timedelta(minutes=5)).replace(tzinfo=None)) == 300   # naive = UTC
    assert freshness_lag_seconds(now - timedelta(hours=3), now) == 0.0   # idle source is not "stale"
