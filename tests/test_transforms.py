"""
Local tests for the lakehouse transforms (databricks/notebooks/transforms.py).

Runs on plain PySpark, no Databricks needed. Silver/gold tables are faked as
temp views named schema_table (e.g. silver_payments), which is what the fq()
resolver below returns.

Run:  python -m pytest tests/test_transforms.py -v   (needs pyspark + Java 17+)

Scenario covered by the fake events:
  payment 1: authorized -> captured -> refunded (+ a duplicate delivery of "captured")
  payment 2: negative amount (bad data)
  payment 3: bad currency "US$" and an unknown column "risk_score" (schema drift)
  merchant 10: fee 2.90% -> 3.20% -> no-op update -> risk tier change (SCD2)
  customer 5: inserted then deleted (tombstone, PII removed)
  refund 2: larger than its payment (bad data)
"""

import json
import os
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "databricks" / "notebooks"))
import transforms as T  # noqa: E402

BASE_MS = 1_759_600_000_000  # 2025-10-04T17:46:40Z
MIN = 60_000


def fq(schema: str, table: str) -> str:
    return f"{schema}_{table}"


@pytest.fixture(scope="module")
def spark():
    # Edge case: Spark workers launch `python3` from PATH, which may be a
    # different minor version than this venv (PYTHON_VERSION_MISMATCH).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[2]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    yield s
    s.stop()


def ts(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_offsets: dict[str, int] = {}


def ev(table, op, row, lsn, t_ms, before=None, offset=None):
    """Build one bronze row exactly like consumer.py writes it."""
    topic = f"payflow.public.{table}"
    if offset is None:
        offset = _offsets.get(topic, 0)
        _offsets[topic] = offset + 1
    pk = T.TABLE_SPECS[table]["pk"]
    key_src = row if row is not None else before
    return {
        "op": op, "table_name": table,
        "primary_key": json.dumps({pk: key_src[pk]}),
        "before": json.dumps(before) if before is not None else None,
        "after": json.dumps(row) if row is not None else None,
        "source_ts_ms": t_ms, "source_lsn": lsn, "source_tx_id": lsn,
        "debezium_ts_ms": t_ms + 100,
        "_kafka_topic": topic, "_kafka_partition": 0, "_kafka_offset": offset,
    }


def merchant(fee, risk="low", country="US"):
    return {"merchant_id": 10, "name": "Acme", "category": "saas", "country": country,
            "risk_tier": risk, "fee_bps": fee, "status": "active",
            "created_at": ts(BASE_MS), "updated_at": ts(BASE_MS)}


def payment(pid, status, amount=10_000, currency="USD", **extra):
    return {"payment_id": pid, "merchant_id": 10, "customer_id": 5, "amount_cents": amount,
            "currency": currency, "status": status, "card_brand": "visa", "failure_reason": None,
            "created_at": ts(BASE_MS + 10 * MIN), "updated_at": ts(BASE_MS + 10 * MIN), **extra}


def build_events():
    _offsets.clear()
    e = []
    # merchant: snapshot @0, fee change @5min, no-op @6min, risk change @30min
    e.append(ev("merchants", "r", merchant(290), 50, BASE_MS))
    e.append(ev("merchants", "u", merchant(320), 150, BASE_MS + 5 * MIN))
    e.append(ev("merchants", "u", merchant(320), 160, BASE_MS + 6 * MIN))
    e.append(ev("merchants", "u", merchant(320, risk="high"), 400, BASE_MS + 30 * MIN))
    # customer inserted then deleted
    cust = {"customer_id": 5, "email": "A@x.com ", "country": "US",
            "created_at": ts(BASE_MS), "updated_at": ts(BASE_MS)}
    e.append(ev("customers", "c", cust, 60, BASE_MS + 1 * MIN))
    e.append(ev("customers", "d", None, 900, BASE_MS + 50 * MIN, before=cust))
    # payment 1 lifecycle; captured at +12min (fee 320 already active)
    e.append(ev("payments", "c", payment(1, "authorized"), 200, BASE_MS + 10 * MIN))
    cap = ev("payments", "u", payment(1, "captured"), 210, BASE_MS + 12 * MIN)
    e.append(cap)
    e.append(dict(cap))  # duplicate delivery (same topic/partition/offset)
    e.append(ev("payments", "u", payment(1, "refunded"), 500, BASE_MS + 40 * MIN))
    # bad payments
    e.append(ev("payments", "c", payment(2, "authorized", amount=-500), 220, BASE_MS + 13 * MIN))
    e.append(ev("payments", "c", payment(3, "authorized", currency="US$", risk_score=7), 230, BASE_MS + 14 * MIN))
    # refunds: good full refund on payment 1, bad oversized refund on payment 1
    r1 = {"refund_id": 1, "payment_id": 1, "amount_cents": 10_000, "reason": "requested_by_customer",
          "status": "pending", "created_at": ts(BASE_MS + 40 * MIN), "updated_at": ts(BASE_MS + 40 * MIN)}
    e.append(ev("refunds", "c", r1, 490, BASE_MS + 40 * MIN))
    e.append(ev("refunds", "u", {**r1, "status": "succeeded"}, 600, BASE_MS + 60 * MIN))
    r2 = {**r1, "refund_id": 2, "amount_cents": 99_999}
    e.append(ev("refunds", "c", r2, 610, BASE_MS + 61 * MIN))
    return e


@pytest.fixture(scope="module")
def silver(spark):
    """Run parse + dedupe + latest-per-key and register silver/ops views."""
    events = spark.createDataFrame(build_events(), schema=(
        "op STRING, table_name STRING, primary_key STRING, before STRING, after STRING, "
        "source_ts_ms BIGINT, source_lsn BIGINT, source_tx_id BIGINT, debezium_ts_ms BIGINT, "
        "_kafka_topic STRING, _kafka_partition INT, _kafka_offset BIGINT"))
    deduped = T.dedupe_events(events)
    out = {"raw_count": events.count(), "dedup_count": deduped.count(), "events": deduped}
    for table, spec in T.TABLE_SPECS.items():
        hist = T.parse_changes(deduped, table)
        state = T.latest_per_key(hist, spec["pk"])
        hist.createOrReplaceTempView(f"silver_{table}_history")
        state.createOrReplaceTempView(f"silver_{table}_state")
        state.filter("NOT _is_deleted").createOrReplaceTempView(f"silver_{table}")
    spark.sql(T.dq_rules_sql(fq)).createOrReplaceTempView("ops_dq_results")
    return out


def test_duplicate_delivery_removed(silver):
    assert silver["raw_count"] == silver["dedup_count"] + 1


def test_current_state_is_latest_event(spark, silver):
    row = spark.sql("SELECT status FROM silver_payments WHERE payment_id = 1").collect()
    assert row[0]["status"] == "refunded"


def test_delete_is_tombstone_without_pii(spark):
    st = spark.sql("SELECT _is_deleted, email_hash FROM silver_customers_state WHERE customer_id = 5").collect()[0]
    assert st["_is_deleted"] is True and st["email_hash"] is None
    assert spark.sql("SELECT COUNT(*) c FROM silver_customers").collect()[0]["c"] == 0
    # history keeps hashed (never raw) email for the insert
    h = spark.sql("SELECT email_hash FROM silver_customers_history WHERE op = 'c'").collect()[0]["email_hash"]
    assert h is not None and len(h) == 64
    assert "email" not in spark.table("silver_customers_history").columns


def test_timestamps_parsed(spark):
    r = spark.sql("SELECT created_at FROM silver_payments WHERE payment_id = 1").collect()[0]
    assert r["created_at"] is not None


def test_schema_drift_detected(silver):
    drift = T.schema_drift(silver["events"], "payments").collect()
    assert [d["column_name"] for d in drift] == ["risk_score"]


def test_dq_rules(spark, silver):
    got = {(r["rule_name"], r["pk"]) for r in spark.table("ops_dq_results").collect()}
    assert ("amount_not_positive", "2") in got
    assert ("invalid_currency", "3") in got
    assert ("refund_exceeds_payment", "2") in got
    # good records are not flagged
    assert not any(pk == "1" and rule.startswith("refund") for rule, pk in got)
    assert ("amount_not_positive", "1") not in got


def test_scd2_versions_and_noop_collapse(spark, silver):
    rows = spark.sql(T.dim_merchant_scd2_sql(fq)).orderBy("valid_from").collect()
    assert [(r["fee_bps"], r["risk_tier"]) for r in rows] == [(290, "low"), (320, "low"), (320, "high")]
    assert [r["is_current"] for r in rows] == [False, False, True]
    assert rows[0]["valid_to"] == rows[1]["valid_from"]  # no gaps


def test_lifecycle_and_point_in_time_fee(spark, silver):
    spark.sql(T.dim_merchant_scd2_sql(fq)).createOrReplaceTempView("gold_dim_merchant_scd2")
    spark.sql(T.fct_payment_lifecycle_sql(fq)).createOrReplaceTempView("gold_fct_payment_lifecycle")
    p1 = spark.sql("SELECT * FROM gold_fct_payment_lifecycle WHERE payment_id = 1").collect()[0]
    assert p1["seconds_to_capture"] == 120
    assert p1["refunded_cents"] == 10_000  # bad refund 2 excluded
    assert p1["is_flagged"] is False
    flagged = {r["payment_id"] for r in spark.sql(
        "SELECT payment_id FROM gold_fct_payment_lifecycle WHERE is_flagged").collect()}
    assert flagged == {2, 3}

    s = spark.sql(T.daily_settlement_sql(fq)).collect()
    assert len(s) == 1
    day = s[0]
    # captured at +12min -> fee 320 bps (the version valid THEN, not the first one)
    assert day["gross_cents"] == 10_000
    assert day["fee_cents"] == 320
    assert day["refund_cents"] == 10_000
    assert day["net_cents"] == 10_000 - 320 - 10_000
    assert day["running_balance_cents"] == day["net_cents"]


def test_merchant_risk_runs(spark, silver):
    r = spark.sql(T.merchant_risk_sql(fq)).collect()
    assert r[0]["current_risk_tier"] == "high"
    assert r[0]["captured_30d"] == 1


def test_catch_rate(spark, silver):
    inj = spark.createDataFrame([
        ("payments", "2", "negative_amount", ts(BASE_MS + 13 * MIN)),
        ("payments", "3", "bad_currency", ts(BASE_MS + 14 * MIN)),
        ("refunds", "2", "refund_exceeds_payment", ts(BASE_MS + 30 * MIN)),
    ], "table_name STRING, pk STRING, kind STRING, injected_at STRING")
    inj.createOrReplaceTempView("ops_injected_bad_records")
    r = spark.sql(T.dq_catch_rate_sql(fq)).collect()[0]
    assert r["injected"] == 3 and r["caught"] == 3 and r["false_positives"] == 0


def test_scd2_first_version_opens_at_created_at(spark):
    """
    Edge case: a merchant first seen through the Debezium SNAPSHOT (op 'r') has
    a first event stamped with the snapshot time. A payment captured before the
    snapshot must still find a fee version (valid_from = created_at).
    """
    snap = {"merchant_id": 77, "name": "Old Co", "category": "saas", "country": "US", "risk_tier": "low",
            "fee_bps": 250, "status": "active", "created_at": ts(BASE_MS - 86_400_000), "updated_at": ts(BASE_MS)}
    rows = [ev("merchants", "r", snap, 10, BASE_MS + 60 * MIN, offset=900)]
    df = spark.createDataFrame(rows, schema=(
        "op STRING, table_name STRING, primary_key STRING, before STRING, after STRING, "
        "source_ts_ms BIGINT, source_lsn BIGINT, source_tx_id BIGINT, debezium_ts_ms BIGINT, "
        "_kafka_topic STRING, _kafka_partition INT, _kafka_offset BIGINT"))
    T.parse_changes(df, "merchants").createOrReplaceTempView("snap_silver_merchants_history")
    dim = spark.sql(T.dim_merchant_scd2_sql(lambda s, t: f"snap_{s}_{t}")).collect()
    assert len(dim) == 1
    assert dim[0]["valid_from"].isoformat().startswith("2025-10-03")   # created_at, a day before the snapshot
    assert dim[0]["is_current"] is True
