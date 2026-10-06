"""
CDC landing consumer: Kafka -> micro-batched Parquet files in ./landing/

Reads Debezium change events from every payflow.public.* topic, buffers them,
and writes one Parquet file per table per flush. Phase 2 uploads these files to
a Databricks Volume, where Auto Loader picks them up into the bronze layer.

Run:
    python consumer/consumer.py                       # defaults: 5,000 events or 30s
    python consumer/consumer.py --batch-size 2000 --flush-seconds 10

=============================================================================
KEY DESIGN DECISIONS
=============================================================================

1. WHY A LOCAL CONSUMER WRITING FILES (instead of Databricks reading Kafka):
   Databricks Free Edition runs on serverless compute in the cloud. It cannot
   reach a Kafka broker on your laptop. So we bridge with files: consume
   locally, write Parquet, upload. This is the micro-batch pattern.
     + works with free tools, and files are easy to inspect and replay
     - latency is "seconds to minutes", not sub-second true streaming
   In production with a cloud Kafka (MSK/Confluent), Databricks would read the
   topic directly with Structured Streaming. Say this in interviews.

2. DELIVERY GUARANTEE: AT-LEAST-ONCE, then dedupe downstream.
   Order of operations on every flush:
       write file to temp name -> atomic rename -> THEN commit Kafka offsets
   If we crash after the rename but before the commit, the next run re-reads
   those events and writes them again = duplicates, but never data loss.
   Every row carries (_kafka_topic, _kafka_partition, _kafka_offset), which is
   globally unique, so bronze/silver can drop duplicates exactly.
   WHY NOT EXACTLY-ONCE: Kafka transactions only give exactly-once when the
   sink is also Kafka. Writing to files, "at-least-once + idempotent dedupe" is
   the standard, honest answer.

3. PAYLOAD STORED AS JSON STRINGS (before/after), not flattened columns.
   Each table has different columns, and source schemas change (someone adds a
   column). If we flattened into typed Parquet columns, a new source column
   would break the writer or silently drop data. Keeping `after`/`before` as
   JSON strings means bronze never breaks on schema drift; silver parses and
   types the fields, and schema-drift detection lives there.
     + bronze is a faithful, replayable copy of the raw events
     - bronze isn't directly queryable column-by-column (silver fixes that)

4. ATOMIC FILE WRITES.
   We write `.parquet.tmp` and rename when complete. Rename is atomic on the
   same filesystem, so the uploader/Auto Loader never sees a half-written file.

5. MANUAL OFFSET COMMITS (enable.auto.commit = false).
   Auto-commit commits on a timer, possibly before the file is on disk. That
   turns a crash into silent data loss. We commit only after files are written.

=============================================================================
EDGE CASES HANDLED
=============================================================================

6. PARTIAL FLUSH FAILURE (disk full, permission error on one table's folder):
   each table's buffer is removed only after ITS file is renamed into place,
   and offsets are committed only if every table succeeded. A retry rewrites
   only what's missing; worst case is duplicates, never loss.

7. REBALANCE (a second consumer joins, or the broker reassigns partitions):
   on_revoke flushes and commits BEFORE the partitions are taken away, so the
   new owner starts exactly where we stopped instead of re-reading a batch.

8. NON-FATAL KAFKA ERRORS (broker restarting, topic not created yet): logged
   and retried by librdkafka. Only fatal errors stop the consumer.

9. POISON MESSAGES (invalid JSON, wrong shape): written to a dead-letter file
   with their offsets, then skipped, so one bad message can't block the stream.

10. STALE .tmp FILES left by a crash mid-write are removed at start-up (only if
    older than 10 minutes, so a parallel consumer's in-flight write is safe).

11. LATENCY IS HONEST: _ingested_at is stamped when the file is written, not
    when the message is polled, so measured latency includes the flush wait.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_PATTERN = "^payflow\\.public\\..*"   # regex: picks up new tables automatically
LANDING_DIR = Path(os.getenv("LANDING_DIR", Path(__file__).resolve().parent.parent / "landing"))
STALE_TMP_SECONDS = 600

# Fixed bronze schema, identical for every table (see decision 3).
BRONZE_SCHEMA = pa.schema([
    ("op", pa.string()),                 # c=create, u=update, d=delete, r=snapshot read
    ("table_name", pa.string()),
    ("primary_key", pa.string()),        # Debezium message key as JSON, e.g. {"payment_id": 42}
    ("before", pa.string()),             # row before the change (JSON), null for inserts
    ("after", pa.string()),              # row after the change (JSON), null for deletes
    ("source_ts_ms", pa.int64()),        # when the change was committed in Postgres
    ("source_lsn", pa.int64()),          # WAL position: strict ordering within the DB
    ("source_tx_id", pa.int64()),        # Postgres transaction id
    ("debezium_ts_ms", pa.int64()),      # when Debezium processed it
    ("_kafka_topic", pa.string()),
    ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()),       # (topic, partition, offset) = unique event id
    ("_ingested_at", pa.timestamp("ms", tz="UTC")),  # when we wrote it to landing
])
VALID_OPS = {"c", "u", "d", "r"}


def parse_event(msg: Message) -> dict | None:
    """
    Turn one Debezium Kafka message into a flat bronze row.
    Returns None for messages that carry no row change (e.g. heartbeats).
    Raises ValueError/TypeError/KeyError for malformed messages (dead-letter).
    """
    # 1. Null value = Kafka tombstone. Disabled in our connector; be defensive.
    if msg.value() is None:
        return None

    # 2. Must be a JSON object. json.loads raises ValueError on bad JSON; a
    #    valid-but-wrong shape (list, number, string) is rejected explicitly.
    value = json.loads(msg.value())
    if not isinstance(value, dict):
        raise TypeError(f"expected a JSON object, got {type(value).__name__}")
    if "op" not in value:
        return None  # not a row-change event
    if value["op"] not in VALID_OPS:
        raise ValueError(f"unknown op {value['op']!r}")

    # 3. Every row change has at least one image; a "c" without "after" or a
    #    "d" without "before" means the envelope is broken.
    if value.get("after") is None and value.get("before") is None:
        raise ValueError("change event without before or after image")

    source = value.get("source") or {}
    return {
        "op": value["op"],
        "table_name": source.get("table") or msg.topic().rsplit(".", 1)[-1],
        "primary_key": msg.key().decode() if msg.key() else None,
        "before": json.dumps(value["before"]) if value.get("before") is not None else None,
        "after": json.dumps(value["after"]) if value.get("after") is not None else None,
        "source_ts_ms": source.get("ts_ms"),
        "source_lsn": source.get("lsn"),
        "source_tx_id": source.get("txId"),
        "debezium_ts_ms": value.get("ts_ms"),
        "_kafka_topic": msg.topic(),
        "_kafka_partition": msg.partition(),
        "_kafka_offset": msg.offset(),
        "_ingested_at": None,   # stamped at flush time (edge case 11)
    }


class LandingWriter:
    """Buffers rows per table and flushes them to Parquet files atomically."""

    def __init__(self, root: Path):
        self.root = root
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.dlq_path = root / "_dead_letter" / "events.jsonl"

    def add(self, row: dict) -> None:
        self.buffers[row["table_name"]].append(row)

    def size(self) -> int:
        return sum(len(rows) for rows in self.buffers.values())

    def dead_letter(self, msg: Message, error: str) -> None:
        """
        Malformed messages go to a dead-letter file instead of crashing the
        consumer or being silently skipped.
        WHY: one poison message shouldn't stop the whole pipeline, but we must
        never lose it either. Someone can inspect and replay it later.
        """
        self.dlq_path.parent.mkdir(parents=True, exist_ok=True)
        with self.dlq_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "error": error,
                "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                "raw_value": msg.value().decode(errors="replace") if msg.value() else None,
                "at": datetime.now(timezone.utc).isoformat(),
            }) + "\n")

    def clean_stale_tmp(self, older_than_s: float = STALE_TMP_SECONDS) -> int:
        """Edge case 10: delete orphaned .tmp files from a crash mid-write."""
        removed, cutoff = 0, time.time() - older_than_s
        for tmp in self.root.glob("*/ingest_date=*/*.parquet.tmp"):
            try:
                if tmp.stat().st_mtime < cutoff:
                    tmp.unlink()
                    removed += 1
            except FileNotFoundError:
                pass  # someone else removed it first
        return removed

    def flush(self) -> int:
        """
        Write every non-empty buffer to its own Parquet file. Returns rows written.
        A table's buffer is dropped only after its file is in place (edge case 6).
        """
        written = 0
        now = datetime.now(timezone.utc)
        for table in list(self.buffers):
            rows = self.buffers[table]
            if not rows:
                del self.buffers[table]
                continue
            # 1. Partition folders by table and ingest date:
            #      landing/payments/ingest_date=2026-10-04/part-...parquet
            #    WHY: lets Auto Loader / Airflow process one table or one day at
            #    a time, and makes backfills of a single day easy.
            out_dir = self.root / table / f"ingest_date={now:%Y-%m-%d}"
            out_dir.mkdir(parents=True, exist_ok=True)

            # 2. Filename includes min/max offsets for traceability: you can tell
            #    exactly which Kafka range a file contains. uuid avoids collisions.
            offsets = [r["_kafka_offset"] for r in rows]
            name = f"part-{now:%H%M%S}-{min(offsets)}-{max(offsets)}-{uuid.uuid4().hex[:8]}.parquet"
            final_path = out_dir / name
            tmp_path = out_dir / (name + ".tmp")

            # 3. Stamp the landing time now: this is when the event becomes visible.
            for r in rows:
                r["_ingested_at"] = now

            # 4. zstd: better compression than snappy at similar speed. Smaller
            #    files = faster uploads to Databricks.
            try:
                pq.write_table(pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA), tmp_path, compression="zstd")
                os.replace(tmp_path, final_path)  # 5. atomic rename
            except BaseException:
                tmp_path.unlink(missing_ok=True)  # never leave a half file behind
                raise
            written += len(rows)
            del self.buffers[table]               # 6. only now is it safe to forget
        return written


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kafka -> Parquet landing consumer")
    # BATCH SIZE / FLUSH INTERVAL TRADEOFF:
    #   bigger batches -> fewer, larger files (Databricks and Parquet love this)
    #   smaller/faster flushes -> lower latency, but many tiny files ("small
    #   file problem": slow listing, slow queries). Flush on whichever comes
    #   first so quiet periods still produce data within --flush-seconds.
    p.add_argument("--batch-size", type=int, default=5000, help="flush after this many events")
    p.add_argument("--flush-seconds", type=float, default=30, help="flush at least this often")
    p.add_argument("--group-id", default="payflow-landing")
    args = p.parse_args(argv)
    if args.batch_size < 1 or args.flush_seconds <= 0:
        p.error("--batch-size must be >= 1 and --flush-seconds > 0")
    return args


def main() -> None:
    args = parse_args()

    # 1. Consumer config.
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        # The group id is how Kafka remembers our position. Change it and you
        # re-read the topics from the start (handy for a full replay).
        "group.id": args.group_id,
        # First run: start from the beginning so we get Debezium's initial
        # snapshot, not just changes made after we started.
        "auto.offset.reset": "earliest",
        # See decision 5: we commit only after files are safely written.
        "enable.auto.commit": False,
        # Ask Kafka for new topics matching the regex every 30s.
        "topic.metadata.refresh.interval.ms": 30000,
    })

    writer = LandingWriter(LANDING_DIR)
    if (n := writer.clean_stale_tmp()):
        print(f"removed {n} stale .tmp files from a previous crash")

    # Highest offset seen per partition since the last commit.
    pending_offsets: dict[tuple[str, int], int] = {}
    last_flush = time.monotonic()
    total = 0
    running = True

    def stop(*_):
        nonlocal running
        running = False

    # 2. Ctrl+C / SIGTERM = graceful: finish the loop, final flush, commit.
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    def flush_and_commit() -> None:
        nonlocal total, last_flush
        if not pending_offsets:
            last_flush = time.monotonic()
            return
        n = writer.flush()
        # CHAOS HOOK (Phase 8): crash after files are written but BEFORE the
        # offset commit. This is the exact window that produces duplicates under
        # at-least-once delivery, so we can prove silver removes them.
        if os.getenv("PAYFLOW_CHAOS_CRASH_BEFORE_COMMIT") == "1":
            print("CHAOS: crashing after write, before commit", flush=True)
            os._exit(137)
        # Commit offset+1: Kafka's committed offset means "next message to read".
        consumer.commit(
            offsets=[TopicPartition(t, p, o + 1) for (t, p), o in pending_offsets.items()],
            asynchronous=False,   # block until Kafka confirms; correctness > speed
        )
        pending_offsets.clear()
        total += n
        last_flush = time.monotonic()
        print(f"[{datetime.now():%H:%M:%S}] flushed {n} events (total {total})", flush=True)

    def on_revoke(_consumer, partitions) -> None:
        # Edge case 7: hand partitions over cleanly.
        if pending_offsets:
            print(f"rebalance: revoking {len(partitions)} partitions, flushing first", flush=True)
            flush_and_commit()

    # 3. Subscribe by regex, with the rebalance hook.
    consumer.subscribe([TOPIC_PATTERN], on_revoke=on_revoke)

    print(f"Consuming {TOPIC_PATTERN} from {BOOTSTRAP} -> {LANDING_DIR}", flush=True)
    try:
        # 4. Poll loop.
        while running:
            msg = consumer.poll(1.0)
            if msg is not None:
                err = msg.error()
                if err:
                    if err.code() == KafkaError._PARTITION_EOF:
                        pass                          # informational, not an error
                    elif err.fatal():
                        raise KafkaException(err)     # edge case 8: give up
                    else:
                        print(f"[warn] kafka: {err}", flush=True)  # librdkafka retries itself
                else:
                    try:
                        row = parse_event(msg)
                        if row:
                            writer.add(row)
                    except (ValueError, KeyError, TypeError) as e:
                        writer.dead_letter(msg, repr(e))   # edge case 9
                    # Track the offset even for skipped/dead-lettered messages
                    # so we don't re-read them forever.
                    pending_offsets[(msg.topic(), msg.partition())] = msg.offset()

            # 5. Flush on size OR time, whichever comes first.
            if writer.size() >= args.batch_size or time.monotonic() - last_flush >= args.flush_seconds:
                flush_and_commit()
    finally:
        # 6. Graceful shutdown: flush what we have so Ctrl+C never loses buffered
        #    events, then leave the consumer group cleanly (faster rebalance).
        try:
            flush_and_commit()
        finally:
            consumer.close()
        print(f"Stopped. Total events landed: {total}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        sys.exit(f"Kafka error: {e}")
