"""
Unit tests for the pure logic in the uploader, reconciliation and loss check.
No Postgres, Kafka or Databricks needed.

Run:  python -m pytest tests/test_pipeline_logic.py -v
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from verify_no_loss import diff_table  # noqa: E402

from reconciliation.reconcile import compare  # noqa: E402
from uploader.upload_to_volume import upload_all  # noqa: E402


# ---------------------------------------------------------------- reconcile
def test_compare_exact_match():
    src = {"payments": [("2026-10-01", 10, 50_000)]}
    lake = {"payments": [("2026-10-01", 10, 50_000)]}
    assert all(r["match"] for r in compare(src, lake))


def test_compare_detects_missing_day_and_cent_difference():
    src = {"payments": [("2026-10-01", 10, 50_000), ("2026-10-02", 5, 100)]}
    lake = {"payments": [("2026-10-01", 10, 49_999)]}
    res = {r["key"]: r for r in compare(src, lake)}
    assert not res["2026-10-01"]["match"]          # off by one cent
    assert res["2026-10-02"]["lake_count"] == 0     # whole day missing
    assert not res["2026-10-02"]["match"]


def test_compare_detects_extra_rows_in_lake():
    res = compare({"refunds": []}, {"refunds": [("2026-10-01", 1, 10)]})
    assert len(res) == 1 and not res[0]["match"]


# ---------------------------------------------------------------- verify_no_loss
def test_diff_table_clean():
    d = diff_table({1: "captured", 2: "failed"}, {1: ("u", "captured"), 2: ("u", "failed"), 3: ("d", None)}, True)
    assert d["n_missing"] == d["n_extra"] == d["n_wrong"] == 0


def test_diff_table_finds_missed_insert_missed_delete_and_stale_update():
    pg = {1: "refunded", 2: "captured"}                       # 3 was deleted in Postgres
    landed = {1: ("u", "captured"), 3: ("c", "authorized")}    # 1 stale, 2 missing, 3 delete missed
    d = diff_table(pg, landed, check_status=True)
    assert d["missing"] == [2] and d["extra"] == [3] and d["wrong_status"] == [1]


# ---------------------------------------------------------------- uploader
class FakeFiles:
    def __init__(self, fail_on=None):
        self.uploaded, self.fail_on = [], fail_on

    def upload(self, path, f, overwrite=False):
        if self.fail_on and self.fail_on in path:
            raise ConnectionError("network down")
        self.uploaded.append((path, len(f.read()), overwrite))


class FakeClient:
    def __init__(self, files):
        self.files = files


def make_landing(tmp_path):
    landing = tmp_path / "landing"
    for t in ("payments", "refunds"):
        d = landing / t / "ingest_date=2026-10-05"
        d.mkdir(parents=True)
        (d / f"part-{t}.parquet").write_bytes(b"x" * 10)
        (d / f"part-{t}.parquet.tmp").write_bytes(b"half")   # in-progress write
    return landing


def test_upload_moves_files_and_skips_tmp(tmp_path, monkeypatch):
    monkeypatch.setenv("PAYFLOW_CATALOG", "payflow")
    landing, archive = make_landing(tmp_path), tmp_path / "archive"
    fake = FakeFiles()
    assert upload_all(FakeClient(fake), landing, archive) == 2
    paths = [p for p, _, _ in fake.uploaded if p.endswith(".parquet")]
    assert paths == sorted(paths) or len(paths) == 2
    assert all(p.startswith("/Volumes/payflow/raw/files/landing/") for p in paths)
    assert all(ow for _, _, ow in fake.uploaded)                     # idempotent re-uploads
    assert not list(landing.glob("*/*/*.parquet"))                   # moved out of landing
    assert len(list(archive.glob("*/*/*.parquet"))) == 2
    assert len(list(landing.glob("*/*/*.tmp"))) == 2                 # never touched


def test_failed_upload_leaves_file_for_retry(tmp_path):
    landing, archive = make_landing(tmp_path), tmp_path / "archive"
    try:
        upload_all(FakeClient(FakeFiles(fail_on="refunds")), landing, archive)
    except ConnectionError:
        pass
    # the refunds file is still in landing, so the next run retries it
    assert [p.parent.parent.name for p in landing.glob("*/*/*.parquet")] == ["refunds"]


# ---------------------------------------------------------------- edge cases
def test_uploader_skips_empty_ground_truth_and_orders_by_path(tmp_path):
    landing, archive = make_landing(tmp_path), tmp_path / "archive"
    gt = tmp_path / "gt.jsonl"
    gt.write_text("")                                   # simulator ran with --bad-rate 0
    fake = FakeFiles()
    upload_all(FakeClient(fake), landing, archive, ground_truth=gt)
    assert not any("ground_truth" in p for p, _, _ in fake.uploaded)


def test_reconcile_normalizes_decimal_and_none_keys():
    from decimal import Decimal
    res = compare({"payments": [(None, 2, Decimal("100"))]}, {"payments": [("None", 2, 100)]})
    assert res[0]["match"]


def test_connector_status_parsing():
    sys.path.insert(0, str(ROOT / "connectors"))
    from register_connector import task_states
    ok = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
    assert task_states(ok) == ("RUNNING", ["RUNNING"])
    assert task_states("not json") == ("UNKNOWN", [])
    assert task_states({"connector": {"state": "RUNNING"}, "tasks": []}) == ("RUNNING", [])


def test_simulator_rejects_bad_cli_values():
    import pytest
    sys.path.insert(0, str(ROOT / "simulator"))
    from simulator import parse_args
    for argv in (["--rate", "0"], ["--bad-rate", "1.5"], ["--duration", "-1"]):
        with pytest.raises(SystemExit):
            parse_args(argv)
    assert parse_args(["--rate", "5"]).rate == 5


def test_benchmark_lag_parser_counts_uncommitted_partitions():
    from benchmark_throughput import parse_group_describe
    out = """
GROUP           TOPIC                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID HOST CLIENT-ID
payflow-landing payflow.public.payments  0          90              100             10    c1 /1 rdkafka
payflow-landing payflow.public.refunds   1          -               40              -     c1 /1 rdkafka
payflow-landing _connect_offsets         0          5               5               0     -  -  -
"""
    assert parse_group_describe(out) == (140, 50)       # 10 lag + 40 never-committed
