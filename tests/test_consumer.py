"""
Unit tests for the landing consumer. No Kafka needed: we fake the Message
object with the exact JSON shape Debezium produces.

Run:  python -m pytest tests -v

WHY TEST THIS PART FIRST: the consumer is where data can be silently lost or
duplicated. Parsing and atomic writes are pure logic, so they're cheap to test
and catch the bugs that would be hardest to notice later in Databricks.
"""

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "consumer"))
from consumer import LandingWriter, parse_event  # noqa: E402


class FakeMessage:
    """Mimics confluent_kafka.Message for the methods the consumer calls."""

    def __init__(self, value, key=b'{"payment_id": 1}', topic="payflow.public.payments",
                 partition=0, offset=0):
        self._value = json.dumps(value).encode() if isinstance(value, dict) else value
        self._key, self._topic, self._partition, self._offset = key, topic, partition, offset

    def value(self): return self._value
    def key(self): return self._key
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset


def debezium_event(op, before=None, after=None, lsn=1000):
    """Shape of a real Debezium Postgres event with schemas disabled."""
    return {
        "before": before, "after": after, "op": op, "ts_ms": 1759600000500,
        "source": {"table": "payments", "ts_ms": 1759600000000, "lsn": lsn, "txId": 77},
    }


def test_update_keeps_before_and_after():
    msg = FakeMessage(debezium_event(
        "u",
        before={"payment_id": 1, "status": "authorized"},
        after={"payment_id": 1, "status": "captured"},
    ), offset=42)
    row = parse_event(msg)
    assert row["op"] == "u"
    assert json.loads(row["before"])["status"] == "authorized"
    assert json.loads(row["after"])["status"] == "captured"
    assert row["_kafka_offset"] == 42
    assert row["source_lsn"] == 1000


def test_delete_has_null_after():
    row = parse_event(FakeMessage(debezium_event("d", before={"payment_id": 1})))
    assert row["op"] == "d"
    assert row["after"] is None
    assert row["before"] is not None


def test_tombstone_and_heartbeat_are_skipped():
    assert parse_event(FakeMessage(None)) is None              # tombstone
    assert parse_event(FakeMessage({"ts_ms": 123})) is None    # heartbeat-like, no "op"


def test_malformed_message_raises_for_dead_letter():
    try:
        parse_event(FakeMessage(b"{not json"))
        raise AssertionError("expected ValueError")
    except ValueError:
        pass  # consumer catches this and writes to the dead-letter file


def test_flush_writes_atomic_parquet_per_table(tmp_path):
    w = LandingWriter(tmp_path)
    for i in range(3):
        w.add(parse_event(FakeMessage(debezium_event("c", after={"payment_id": i}), offset=i)))
    w.add(parse_event(FakeMessage(debezium_event("c", after={"refund_id": 9}),
                                  topic="payflow.public.refunds", offset=0)
                      ) | {"table_name": "refunds"})

    assert w.flush() == 4
    assert w.size() == 0

    files = sorted(tmp_path.glob("*/ingest_date=*/*.parquet"))
    assert [f.parent.parent.name for f in files] == ["payments", "refunds"]
    assert not list(tmp_path.rglob("*.tmp"))  # no half-written files left behind
    assert pq.read_table(files[0]).num_rows == 3
    assert "-0-2-" in files[0].name  # min/max offsets in filename


def test_dead_letter_file(tmp_path):
    w = LandingWriter(tmp_path)
    w.dead_letter(FakeMessage(b"{bad", offset=5), "ValueError")
    line = json.loads((tmp_path / "_dead_letter" / "events.jsonl").read_text())
    assert line["offset"] == 5 and line["raw_value"] == "{bad"


# ---------------------------------------------------------------- edge cases
def test_ingested_at_stamped_at_flush_time(tmp_path):
    """Latency must include the flush wait: _ingested_at is set when the file is written."""
    w = LandingWriter(tmp_path)
    row = parse_event(FakeMessage(debezium_event("c", after={"payment_id": 1})))
    assert row["_ingested_at"] is None
    w.add(row)
    w.flush()
    f = next(tmp_path.glob("payments/*/*.parquet"))
    assert pq.read_table(f).column("_ingested_at")[0].as_py() is not None


def test_non_object_json_and_unknown_op_are_dead_lettered():
    import pytest
    for bad in (b"[1, 2]", b"42", b'"op"'):
        with pytest.raises((TypeError, ValueError)):
            parse_event(FakeMessage(bad))
    with pytest.raises(ValueError):
        parse_event(FakeMessage({"op": "x", "after": {"payment_id": 1}}))
    with pytest.raises(ValueError):
        parse_event(FakeMessage({"op": "c", "before": None, "after": None}))


def test_partial_flush_failure_keeps_unwritten_buffers(tmp_path, monkeypatch):
    """If one table's write fails, written tables are dropped from the buffer, the rest stay."""
    import pytest

    w = LandingWriter(tmp_path)
    w.add(parse_event(FakeMessage(debezium_event("c", after={"payment_id": 1}))))
    w.add(parse_event(FakeMessage(debezium_event("c", after={"refund_id": 1}),
                                  topic="payflow.public.refunds")) | {"table_name": "refunds"})
    real_write = pq.write_table

    def flaky(table, path, **kw):
        if "refunds" in str(path):
            raise OSError("disk full")
        return real_write(table, path, **kw)

    monkeypatch.setattr("consumer.pq.write_table", flaky)
    with pytest.raises(OSError):
        w.flush()
    assert list(w.buffers) == ["refunds"]                 # payments written and forgotten
    assert not list(tmp_path.rglob("*.tmp"))               # no half file left behind
    monkeypatch.setattr("consumer.pq.write_table", real_write)
    assert w.flush() == 1                                  # retry writes only what's missing


def test_stale_tmp_cleanup(tmp_path):
    import os
    import time
    d = tmp_path / "payments" / "ingest_date=2026-10-05"
    d.mkdir(parents=True)
    old, fresh = d / "a.parquet.tmp", d / "b.parquet.tmp"
    old.write_bytes(b"x")
    fresh.write_bytes(b"x")
    os.utime(old, (time.time() - 3600, time.time() - 3600))
    assert LandingWriter(tmp_path).clean_stale_tmp() == 1
    assert not old.exists() and fresh.exists()             # in-flight write untouched


def test_consumer_rejects_bad_cli_values():
    import pytest

    from consumer import parse_args
    with pytest.raises(SystemExit):
        parse_args(["--batch-size", "0"])
    with pytest.raises(SystemExit):
        parse_args(["--flush-seconds", "0"])
