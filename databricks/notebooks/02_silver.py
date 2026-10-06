# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: dedupe, type, and apply CDC events
# MAGIC
# MAGIC For each source table this notebook maintains:
# MAGIC - `silver.<table>_history`: every change event, typed, exactly once (audit trail, SCD2 input)
# MAGIC - `silver.<table>_state`: one row per key, current state, deletes kept as **tombstones**
# MAGIC - `silver.<table>` view: `_state` without tombstones (what analysts query)
# MAGIC
# MAGIC **Incremental by high-watermark** on `bronze._bronze_loaded_at`, stored in `ops.watermarks`.
# MAGIC Why not a streaming `foreachBatch`: on serverless (Spark Connect) the batch function runs
# MAGIC in a separate process, and importing our `transforms` module there is fragile. A watermark is
# MAGIC plain batch Spark, visible in a table, and reset with one SQL statement.
# MAGIC Safe because bronze and silver run one after another in a single job with
# MAGIC `max_concurrent_runs = 1`, so no bronze write can land "behind" the watermark.
# MAGIC
# MAGIC **Idempotent:** re-running on the same events changes nothing (history MERGE is insert-only
# MAGIC on the event id; state MERGE only applies strictly newer events). So a failed run is retried
# MAGIC by running it again, with no manual cleanup.
# MAGIC
# MAGIC Steps: 1. params  2. optional full refresh  3. read batch above watermark  4. per table:
# MAGIC history MERGE, state MERGE, drift, view  5. advance watermark

# COMMAND ----------

# 1. Parameters.
dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"

# COMMAND ----------

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import transforms as T  # workspace file in the same folder (deployed by databricks/deploy.py)


def fq(schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


BRONZE = fq("bronze", "cdc_events")
STAGE = fq("ops", "_silver_batch")
run_started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# 2. Full refresh = rebuild silver from bronze. Used for replays after a logic fix
#    and for the determinism test. Bronze is untouched (it's the source of truth).
if full_refresh:
    for table in T.TABLE_SPECS:
        spark.sql(f"DROP VIEW IF EXISTS {fq('silver', table)}")
        spark.sql(f"DROP TABLE IF EXISTS {fq('silver', table + '_history')}")
        spark.sql(f"DROP TABLE IF EXISTS {fq('silver', table + '_state')}")
    spark.sql(f"DELETE FROM {fq('ops', 'watermarks')} WHERE pipeline = 'silver'")
    print("Full refresh: silver dropped, watermark reset")

# COMMAND ----------

# 3. Everything loaded into bronze since the last successful silver run.
wm_row = spark.sql(f"SELECT MAX(value) AS v FROM {fq('ops', 'watermarks')} WHERE pipeline = 'silver'").first()
watermark = wm_row["v"] if wm_row else None

new_events = spark.table(BRONZE)
if watermark:
    new_events = new_events.filter(F.col("_bronze_loaded_at") > F.lit(watermark).cast("timestamp"))

# Materialize the batch once. Serverless doesn't allow df.cache(), and without
# this, every table below would re-scan bronze (5 tables x several actions).
T.dedupe_events(new_events).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE)
batch = spark.table(STAGE)

high = batch.agg(F.max("_bronze_loaded_at").cast("string").alias("h")).first()["h"]
events_processed = batch.count()
dbutils.jobs.taskValues.set(key="events_processed", value=events_processed)
dbutils.jobs.taskValues.set(key="run_started_at", value=run_started_at)
print(f"watermark={watermark} new_events={events_processed}")

# COMMAND ----------


def ensure_table(name, df):
    if not spark.catalog.tableExists(name):
        df.limit(0).write.saveAsTable(name)


# 4. Per table. Tables and views are created even when a table has no events in
#    this batch (edge case: on a young pipeline `disputes` may have none yet,
#    and quality/gold SQL reference every silver view).
for table, spec in T.TABLE_SPECS.items():
    pk = spec["pk"]
    changes = T.parse_changes(batch, table)
    hist, state = fq("silver", f"{table}_history"), fq("silver", f"{table}_state")
    ensure_table(hist, changes)
    ensure_table(state, changes)
    spark.sql(f"CREATE OR REPLACE VIEW {fq('silver', table)} AS SELECT * FROM {state} WHERE NOT _is_deleted")
    if events_processed == 0 or changes.isEmpty():
        continue

    # 4a. History: insert each event exactly once, keyed by Kafka event id.
    #     Insert-only MERGE (instead of append) makes replays and retries safe.
    (DeltaTable.forName(spark, hist).alias("t")
        .merge(changes.alias("s"),
               "t._kafka_topic = s._kafka_topic AND t._kafka_partition = s._kafka_partition "
               "AND t._kafka_offset = s._kafka_offset")
        .withSchemaEvolution()        # new contract columns are added automatically
        .whenNotMatchedInsertAll()
        .execute())

    # 4b. State: apply only the newest event per key, and only if it is newer
    #     than what we have (LSN order). Deletes are written as tombstones
    #     instead of removing the row.
    #     WHY TOMBSTONES: with at-least-once delivery, an old duplicate
    #     "insert" can arrive AFTER the delete was applied. With a hard delete
    #     there's no row to compare against, so the old insert would bring the
    #     deleted customer back. The tombstone's LSN blocks it.
    latest = T.latest_per_key(changes, pk)
    (DeltaTable.forName(spark, state).alias("t")
        .merge(latest.alias("s"), f"t.{pk} = s.{pk}")
        .withSchemaEvolution()
        .whenMatchedUpdateAll(condition=T.NEWER)
        .whenNotMatchedInsertAll()
        .execute())

    # 4c. Schema drift: record unknown columns (insert-only).
    drift = T.schema_drift(batch, table)
    if not drift.isEmpty():
        (DeltaTable.forName(spark, fq("ops", "schema_drift_events")).alias("t")
            .merge(drift.alias("s"), "t.table_name = s.table_name AND t.column_name = s.column_name")
            .whenNotMatchedInsertAll()
            .execute())
        print(f"SCHEMA DRIFT in {table}: {[r['column_name'] for r in drift.collect()]}")

    print(f"{table}: {changes.count()} events applied")

# COMMAND ----------

# 5. Advance the watermark only after every table succeeded. If anything above
#    failed, the next run reprocesses the same events, which is safe (idempotent).
if events_processed > 0 and high:
    spark.sql(f"""
        MERGE INTO {fq('ops', 'watermarks')} t
        USING (SELECT 'silver' AS pipeline, '{high}' AS value) s ON t.pipeline = s.pipeline
        WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (pipeline, value, updated_at) VALUES (s.pipeline, s.value, current_timestamp())
    """)
    print(f"watermark advanced to {high}")
