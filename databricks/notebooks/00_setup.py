# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: schemas, volumes, bronze + ops tables
# MAGIC Idempotent (`IF NOT EXISTS` everywhere), so it runs as the first task of every job run.
# MAGIC Cost is a few metadata calls. In exchange, a fresh workspace never fails on a missing table.
# MAGIC
# MAGIC Steps: 1. widgets  2. schemas + volumes + landing folder  3. bronze table  4. ops tables

# COMMAND ----------

# 1. Job parameters arrive as widgets.
dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# 2. Medallion layers as schemas. WHY SCHEMAS, not table-name prefixes: permissions
#    can be granted per layer (analysts get gold only), and names stay short.
for schema in ["raw", "bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Volumes: governed file storage inside Unity Catalog. Free Edition has limited
# DBFS access, so files and checkpoints live in volumes.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.files")         # landing + ground truth
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")   # Auto Loader state

# Edge case: Auto Loader fails on a path that doesn't exist yet (first run,
# before any upload). An empty folder is fine.
dbutils.fs.mkdirs(f"/Volumes/{catalog}/raw/files/landing")

# COMMAND ----------

# 3. Bronze table with an explicit schema (same columns consumer.py writes, plus
#    two lineage columns). Created up front so downstream tasks never hit
#    "table not found" on a run where no files have arrived yet.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.cdc_events (
        op STRING, table_name STRING, primary_key STRING, before STRING, after STRING,
        source_ts_ms BIGINT, source_lsn BIGINT, source_tx_id BIGINT, debezium_ts_ms BIGINT,
        _kafka_topic STRING, _kafka_partition INT, _kafka_offset BIGINT, _ingested_at TIMESTAMP,
        _source_file STRING, _bronze_loaded_at TIMESTAMP
    )
""")

# COMMAND ----------

# 4. Ops tables: everything needed to PROVE the pipeline works.
ops_tables = {
    # High-watermark for incremental silver processing.
    "watermarks": "pipeline STRING, value STRING, updated_at TIMESTAMP",
    # One row per data quality violation (insert-only).
    "dq_results": "rule_name STRING, table_name STRING, pk STRING, observed STRING, first_detected_at TIMESTAMP",
    # Unknown source columns seen in CDC events.
    "schema_drift_events": "table_name STRING, column_name STRING, first_seen_lsn BIGINT, detected_at TIMESTAMP",
    # Catch rate of DQ rules vs simulator ground truth, per run.
    "dq_catch_rate_history": "run_ts TIMESTAMP, injected BIGINT, caught BIGINT, false_positives BIGINT, flagged_total BIGINT",
    # Pipeline health per run: volume, freshness, latency.
    "pipeline_runs": ("run_ts TIMESTAMP, events_processed BIGINT, bronze_rows BIGINT, max_source_ts TIMESTAMP, "
                      "freshness_seconds DOUBLE, latency_p50_s DOUBLE, latency_p95_s DOUBLE, latency_p99_s DOUBLE"),
    # Written by Airflow's reconciliation task.
    "reconciliation_runs": "run_ts TIMESTAMP, mode STRING, checks INT, mismatches INT, details STRING",
    # Written by Airflow's maintenance task (file counts before/after OPTIMIZE).
    "maintenance_runs": "run_ts TIMESTAMP, table_name STRING, files_before BIGINT, files_after BIGINT, size_bytes BIGINT",
}
for name, ddl in ops_tables.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.ops.{name} ({ddl})")

print("Setup complete")
