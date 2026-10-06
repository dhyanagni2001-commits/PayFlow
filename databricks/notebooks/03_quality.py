# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Quality: run DQ rules, measure catch rate against ground truth
# MAGIC
# MAGIC Runs **after silver, before gold**, so gold can exclude flagged records.
# MAGIC Rules live in `transforms.dq_rules_sql` (unit tested locally).
# MAGIC
# MAGIC **Why measure catch rate:** "we have data quality checks" is a claim. "Rules caught X of Y
# MAGIC injected bad records with Z false positives" is a measurement. The simulator logs every bad
# MAGIC record it injects (ground truth), and we score the rules against it.

# COMMAND ----------

dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import transforms as T


def fq(schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


if not spark.catalog.tableExists(fq("silver", "payments_state")):
    dbutils.notebook.exit("silver not built yet")

# COMMAND ----------

# 1. Replay edge case: silver was rebuilt from bronze, so flags are rebuilt too.
#    Otherwise a rule fixed in transforms.py would keep its old false flags.
if full_refresh:
    spark.sql(f"DELETE FROM {fq('ops', 'dq_results')}")

# 2. Insert-only: a violation is recorded once with the time we first saw it.
# Tradeoff: if a bad record is later corrected in the source, the flag stays.
# Fine for a demo; production would re-evaluate and mark flags resolved.
violations = spark.sql(T.dq_rules_sql(fq)).withColumn("first_detected_at", F.current_timestamp())
(DeltaTable.forName(spark, fq("ops", "dq_results")).alias("t")
    .merge(violations.alias("s"),
           "t.rule_name = s.rule_name AND t.table_name = s.table_name AND t.pk = s.pk")
    .whenNotMatchedInsertAll()
    .execute())

display(spark.sql(f"SELECT rule_name, COUNT(*) AS violations FROM {fq('ops', 'dq_results')} GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# 3. Ground truth uploaded by the uploader. Absent until the simulator injects something.
GT = f"/Volumes/{catalog}/raw/files/ground_truth/bad_records.jsonl"
try:
    gt = spark.read.json(GT).withColumnRenamed("table", "table_name").withColumn("pk", F.col("pk").cast("string"))
    gt.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("ops", "injected_bad_records"))
except Exception as e:  # file not uploaded yet
    print(f"No ground truth yet: {e}")

# 4. Score the rules: recall (caught / injected) and false positives.
if spark.catalog.tableExists(fq("ops", "injected_bad_records")):
    rate = spark.sql(T.dq_catch_rate_sql(fq)).withColumn("run_ts", F.current_timestamp())
    rate.select("run_ts", "injected", "caught", "false_positives", "flagged_total") \
        .write.mode("append").saveAsTable(fq("ops", "dq_catch_rate_history"))
    display(rate)
