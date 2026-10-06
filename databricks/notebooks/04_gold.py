# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Gold: business tables for Tableau + pipeline health
# MAGIC
# MAGIC | Table | Question it answers |
# MAGIC |---|---|
# MAGIC | `dim_merchant_scd2` | What were a merchant's fee and risk tier on any given date? |
# MAGIC | `fct_payment_lifecycle` | When was each payment authorized, captured, refunded, disputed? |
# MAGIC | `daily_merchant_settlement` | How much is each merchant owed per day (point-in-time fees)? |
# MAGIC | `merchant_risk_30d` | Which merchants have high chargeback / refund rates? |
# MAGIC
# MAGIC **Why full rebuild (`CREATE OR REPLACE`) every run:** at this size a rebuild takes seconds and is
# MAGIC always correct, even when a refund arrives days late. Incremental gold (MERGE on affected dates)
# MAGIC is the right move at billions of rows; here it would only add bugs.

# COMMAND ----------

dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import transforms as T


def fq(schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


if not spark.catalog.tableExists(fq("silver", "payments_state")):
    dbutils.notebook.exit("silver not built yet")

# 1. Rebuild every gold table. Order matters: settlement and risk read the
#    dimension and the lifecycle fact.
GOLD = [
    ("dim_merchant_scd2", T.dim_merchant_scd2_sql),
    ("fct_payment_lifecycle", T.fct_payment_lifecycle_sql),
    ("daily_merchant_settlement", T.daily_settlement_sql),
    ("merchant_risk_30d", T.merchant_risk_sql),
]
for name, build in GOLD:
    spark.sql(f"CREATE OR REPLACE TABLE {fq('gold', name)} AS {build(fq)}")
    print(f"gold.{name}: {spark.table(fq('gold', name)).count()} rows")

# COMMAND ----------

# 2. Pipeline health row. Latency = Postgres commit -> row visible in silver, for
# events processed in THIS run. This is true end-to-end freshness: consumer
# flush + Airflow schedule wait + upload + job runtime.
events_processed = dbutils.jobs.taskValues.get(taskKey="silver", key="events_processed", default=0, debugValue=0)
run_started_at = dbutils.jobs.taskValues.get(taskKey="silver", key="run_started_at",
                                            default="1970-01-01 00:00:00", debugValue="1970-01-01 00:00:00")

latency_union = " UNION ALL ".join(
    f"SELECT source_ts_ms, _processed_at FROM {fq('silver', t + '_history')} "
    f"WHERE op <> 'r' AND _processed_at >= TIMESTAMP '{run_started_at}'"
    for t in T.TABLE_SPECS
    if spark.catalog.tableExists(fq("silver", t + "_history"))
)
max_ts_union = " UNION ALL ".join(
    f"SELECT MAX(_source_ts) AS m FROM {fq('silver', t + '_history')}"
    for t in T.TABLE_SPECS
    if spark.catalog.tableExists(fq("silver", t + "_history"))
)

spark.sql(f"""
    INSERT INTO {fq('ops', 'pipeline_runs')}
    SELECT current_timestamp(),
           {int(events_processed)},
           (SELECT COUNT(*) FROM {fq('bronze', 'cdc_events')}),
           m.max_source_ts,
           unix_timestamp(current_timestamp()) - unix_timestamp(m.max_source_ts),
           l.p50, l.p95, l.p99
    FROM (SELECT MAX(m) AS max_source_ts FROM ({max_ts_union})) m
    CROSS JOIN (
        SELECT percentile_approx(sec, 0.50) AS p50,
               percentile_approx(sec, 0.95) AS p95,
               percentile_approx(sec, 0.99) AS p99
        FROM (SELECT (unix_millis(_processed_at) - source_ts_ms) / 1000.0 AS sec FROM ({latency_union}))
    ) l
""")
display(spark.sql(f"SELECT * FROM {fq('ops', 'pipeline_runs')} ORDER BY run_ts DESC LIMIT 5"))
