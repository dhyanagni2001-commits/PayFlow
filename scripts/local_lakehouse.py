"""
Run the lakehouse logic (silver -> quality -> gold) on LOCAL Spark over the
real landed Parquet files, then reconcile silver against Postgres to the cent.

Run (stack up, simulator stopped, consumer drained):
    python scripts/local_lakehouse.py
    python scripts/local_lakehouse.py --no-reconcile      # Postgres not running

WHY THIS EXISTS (not in the original spec):
    The Databricks half needs a workspace and credentials. This script runs the
    SAME transforms.py functions the notebooks use, on the same landed files,
    so the whole pipeline's logic can be exercised end to end on a laptop and
    in CI, and the DQ / SCD2 / settlement / reconciliation numbers can be
    measured before Databricks is set up.

WHAT IS DIFFERENT FROM DATABRICKS (be honest about it):
    - no Delta MERGE: state = latest_per_key over the FULL history. That's the
      state the incremental MERGE converges to, but the MERGE code path itself
      is only exercised in the workspace.
    - no Auto Loader: all files are read every run (fine at laptop scale).
    - output is plain Parquet in ./local_lakehouse/, not Delta tables.

Steps:
    1. bronze:  read landing/ + landing_archive/
    2. silver:  dedupe -> parse -> history + state + view per table, schema drift
    3. quality: DQ rules, catch rate vs simulator ground truth
    4. gold:    SCD2, lifecycle, settlement, risk
    5. replay:  rebuild from duplicated + shuffled events, totals must be identical
    6. reconcile: silver vs Postgres (full mode + daily mode incl. today)
    7. write gold + results.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "databricks" / "notebooks"))

LANDING = Path(os.getenv("LANDING_DIR", ROOT / "landing"))
ARCHIVE = Path(os.getenv("ARCHIVE_DIR", ROOT / "landing_archive"))
GROUND_TRUTH = Path(os.getenv("GROUND_TRUTH", ROOT / "simulator" / "injected" / "bad_records.jsonl"))
OUT = ROOT / "local_lakehouse"

GOLD = ["dim_merchant_scd2", "fct_payment_lifecycle", "daily_merchant_settlement", "merchant_risk_30d"]


def fq(schema: str, table: str) -> str:
    """Local resolver: silver.payments -> temp view silver_payments."""
    return f"{schema}_{table}"


def spark_session():
    # Spark workers must use this interpreter, not whatever `python3` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[4]").appName("payflow-local")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "2g")
            .getOrCreate())


def build(spark, events, T) -> dict:
    """Steps 2-4 on a DataFrame of bronze events. Returns settlement totals per currency."""
    from pyspark.sql import functions as F

    # 2. silver
    deduped = T.dedupe_events(events)
    for table, spec in T.TABLE_SPECS.items():
        hist = T.parse_changes(deduped, table)
        state = T.latest_per_key(hist, spec["pk"])
        hist.createOrReplaceTempView(fq("silver", f"{table}_history"))
        state.createOrReplaceTempView(fq("silver", f"{table}_state"))
        state.filter("NOT _is_deleted").createOrReplaceTempView(fq("silver", table))

    # 3. quality (materialized: gold reads it several times)
    spark.sql(T.dq_rules_sql(fq)).withColumn("first_detected_at", F.current_timestamp()) \
        .localCheckpoint().createOrReplaceTempView(fq("ops", "dq_results"))

    # 4. gold, in dependency order (each one materialized for the next)
    for name, sql in [("dim_merchant_scd2", T.dim_merchant_scd2_sql), ("fct_payment_lifecycle", T.fct_payment_lifecycle_sql),
                      ("daily_merchant_settlement", T.daily_settlement_sql), ("merchant_risk_30d", T.merchant_risk_sql)]:
        spark.sql(sql(fq)).localCheckpoint().createOrReplaceTempView(fq("gold", name))

    rows = spark.sql("""SELECT currency, SUM(captured_count) n, SUM(gross_cents) gross, SUM(fee_cents) fee,
                               SUM(refund_cents) refunds, SUM(dispute_lost_cents) chargebacks, SUM(net_cents) net
                        FROM gold_daily_merchant_settlement GROUP BY currency ORDER BY currency""").collect()
    return {r["currency"]: {k: int(r[k] or 0) for k in ("n", "gross", "fee", "refunds", "chargebacks", "net")} for r in rows}


def reconcile_local(spark) -> dict:
    """Step 6: reuse reconcile.py's queries and compare(), with silver read locally."""
    from payflow_common.connections import catalog, pg_connect
    from reconciliation.reconcile import compare, daily_queries, full_queries

    out = {}
    today = datetime.now(timezone.utc).date()
    # daily mode incl. today: valid here because the simulator is stopped.
    for mode, (pg_sql, lake_sql) in {"full": full_queries(),
                                     "daily": daily_queries(today - timedelta(days=3), today + timedelta(days=1))}.items():
        with pg_connect() as conn, conn.cursor() as cur:
            source = {}
            for t, q in pg_sql.items():
                cur.execute(q)
                source[t] = cur.fetchall()
        lake = {t: [tuple(r) for r in spark.sql(q.replace(f"{catalog()}.silver.", "silver_")).collect()]
                for t, q in lake_sql.items()}
        res = compare(source, lake)
        bad = [r for r in res if not r["match"]]
        out[mode] = {"checks": len(res), "mismatches": len(bad), "samples": bad[:5],
                     "payments_cents": sum(r["source_cents"] for r in res if r["table"] == "payments")}
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-reconcile", action="store_true", help="skip the Postgres comparison")
    args = ap.parse_args()

    import transforms as T
    from pyspark.sql import functions as F

    # 1. bronze
    globs = [str(d / "*" / "ingest_date=*" / "*.parquet") for d in (LANDING, ARCHIVE) if any(d.glob("*/ingest_date=*/*.parquet"))]
    if not globs:
        raise SystemExit("No landed Parquet files. Run the simulator + consumer first.")
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    bronze = spark.read.parquet(*globs).localCheckpoint()
    results: dict = {"run_at": datetime.now(timezone.utc).isoformat(), "bronze_rows": bronze.count()}
    results["bronze_duplicates"] = results["bronze_rows"] - bronze.dropDuplicates(T.EVENT_ID).count()

    # 2-4
    totals = build(spark, bronze, T)
    results["settlement_totals"] = totals

    def count(sql):
        return spark.sql(sql).collect()[0][0]

    results["silver"] = {t: {"history": count(f"SELECT COUNT(*) FROM silver_{t}_history"),
                             "live": count(f"SELECT COUNT(*) FROM silver_{t}"),
                             "tombstones": count(f"SELECT COUNT(*) FROM silver_{t}_state WHERE _is_deleted")}
                         for t in T.TABLE_SPECS}
    results["silver_history_duplicates"] = count(
        "SELECT COUNT(*) - COUNT(DISTINCT _kafka_topic, _kafka_partition, _kafka_offset) FROM silver_payments_history")
    results["schema_drift"] = [r.asDict() for t in T.TABLE_SPECS
                               for r in T.schema_drift(bronze, t).select("table_name", "column_name").collect()]
    results["dq_violations_by_rule"] = {r[0]: r[1] for r in spark.sql(
        "SELECT rule_name, COUNT(*) FROM ops_dq_results GROUP BY 1 ORDER BY 1").collect()}

    # 3b. catch rate vs ground truth
    if GROUND_TRUTH.exists() and GROUND_TRUTH.stat().st_size > 0:
        spark.read.json(str(GROUND_TRUTH)).withColumnRenamed("table", "table_name") \
            .withColumn("pk", F.col("pk").cast("string")).createOrReplaceTempView("ops_injected_bad_records")
        r = spark.sql(T.dq_catch_rate_sql(fq)).collect()[0].asDict()
        r["catch_rate_pct"] = round(100.0 * r["caught"] / r["injected"], 2) if r["injected"] else None
        results["dq_catch_rate"] = r

    results["gold_rows"] = {g: count(f"SELECT COUNT(*) FROM gold_{g}") for g in GOLD}
    results["scd2_historical_versions"] = count("SELECT COUNT(*) FROM gold_dim_merchant_scd2 WHERE NOT is_current")
    results["payments_dollars_in_silver"] = count("SELECT COALESCE(SUM(amount_cents), 0) FROM silver_payments") / 100

    # 7a. write gold before the replay overwrites the views
    OUT.mkdir(exist_ok=True)
    for g in GOLD:
        spark.table(f"gold_{g}").write.mode("overwrite").parquet(str(OUT / g))

    # 5. replay determinism: re-deliver ~20% of events again and shuffle the order.
    noisy = bronze.unionByName(bronze.sample(fraction=0.2, seed=7)).orderBy(F.rand(seed=11))
    replay = build(spark, noisy, T)
    results["replay_identical"] = replay == totals

    # 6. reconcile
    if not args.no_reconcile:
        results["reconciliation"] = reconcile_local(spark)

    (OUT / "results.json").write_text(json.dumps(results, indent=2, default=str))

    # Summary
    print("\n=== PayFlow local lakehouse ===")
    print(f"1. bronze rows: {results['bronze_rows']}  (duplicates in bronze: {results['bronze_duplicates']})")
    print(f"2. silver payments_history duplicates: {results['silver_history_duplicates']}")
    for t, v in results["silver"].items():
        print(f"     {t:10s} history={v['history']:>7} live={v['live']:>6} tombstones={v['tombstones']}")
    print(f"   schema drift: {results['schema_drift'] or 'none'}")
    print(f"3. DQ violations by rule: {results['dq_violations_by_rule']}")
    if "dq_catch_rate" in results:
        c = results["dq_catch_rate"]
        print(f"   catch rate: {c['caught']}/{c['injected']} = {c['catch_rate_pct']}%  false positives={c['false_positives']}")
    print(f"4. gold rows: {results['gold_rows']}  SCD2 historical versions: {results['scd2_historical_versions']}")
    print(f"   payments in silver: ${results['payments_dollars_in_silver']:,.2f}")
    for cur, v in totals.items():
        print(f"     {cur:>4}: captured={v['n']} gross={v['gross']} fee={v['fee']} refunds={v['refunds']} "
              f"chargebacks={v['chargebacks']} net={v['net']}")
    print(f"5. replay (duplicated + shuffled) gives identical gold totals: {results['replay_identical']}")
    ok = results["replay_identical"] and results["silver_history_duplicates"] == 0
    if "reconciliation" in results:
        for mode, r in results["reconciliation"].items():
            print(f"6. reconcile {mode}: {r['checks']} checks, {r['mismatches']} mismatches, "
                  f"${r['payments_cents'] / 100:,.2f} of payments compared")
            for s in r["samples"]:
                print(f"     MISMATCH {s}")
            ok &= r["mismatches"] == 0
    print(f"7. wrote {OUT}/ (gold parquet + results.json)")
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
