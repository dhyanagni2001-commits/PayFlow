# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: Auto Loader, landing Parquet files to a Delta table
# MAGIC
# MAGIC **What bronze is:** an append-only, never-modified copy of every CDC event, plus lineage
# MAGIC columns. If silver or gold have a bug, we fix the code and replay from here.
# MAGIC
# MAGIC **Why Auto Loader** instead of `spark.read.parquet(folder)`:
# MAGIC it remembers which files it already loaded (in the checkpoint), so each run only reads new
# MAGIC files. A plain read would re-read everything every run, or need hand-written file tracking.
# MAGIC
# MAGIC **Why `availableNow`:** serverless compute only supports `availableNow`/`once` triggers.
# MAGIC That fits anyway: Airflow starts a run, it processes everything new, then stops and
# MAGIC releases compute. Cost: freshness is bounded by the Airflow schedule, not sub-second.
# MAGIC
# MAGIC **Why an explicit schema** (not inference): inference fails on an empty folder (first run),
# MAGIC and the landing schema is fixed by `consumer.py` anyway. Source schema drift can't change it:
# MAGIC new source columns live inside the `after`/`before` JSON strings.

# COMMAND ----------

# 1. Parameters and paths.
dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{catalog}/raw/files/landing/"
CHECKPOINT = f"/Volumes/{catalog}/ops/checkpoints/bronze_cdc_events"
TARGET = f"{catalog}.bronze.cdc_events"

# Must match consumer.BRONZE_SCHEMA.
LANDING_SCHEMA = (
    "op STRING, table_name STRING, primary_key STRING, before STRING, after STRING, "
    "source_ts_ms BIGINT, source_lsn BIGINT, source_tx_id BIGINT, debezium_ts_ms BIGINT, "
    "_kafka_topic STRING, _kafka_partition INT, _kafka_offset BIGINT, _ingested_at TIMESTAMP"
)

# COMMAND ----------

from pyspark.sql import functions as F

# 2. Incremental load of every new file, then stop.
query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(LANDING_SCHEMA)
    # Only Parquet. Dead-letter and ground-truth JSONL files are not CDC events.
    .option("pathGlobFilter", "*.parquet")
    # ingest_date=... folders would otherwise be added as a partition column.
    .option("cloudFiles.partitionColumns", "")
    .load(LANDING)
    # 3. Lineage: which file each row came from. Answers "where did this row come from?"
    .withColumn("_source_file", F.col("_metadata.file_path"))
    # 4. Load time, used as silver's incremental high-watermark.
    .withColumn("_bronze_loaded_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()

# COMMAND ----------

n = spark.table(TARGET).count()
print(f"bronze rows total: {n}")
