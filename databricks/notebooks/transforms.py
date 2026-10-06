"""
Shared transformation logic for the PayFlow lakehouse.

Imported by the Databricks notebooks (deployed as a workspace file in the same
folder, so `import transforms` works) AND by local unit tests with plain PySpark.

WHY A SHARED MODULE INSTEAD OF LOGIC INSIDE NOTEBOOKS:
    Notebooks are hard to unit test and easy to copy-paste between. Pure
    functions that take and return DataFrames / SQL strings can be tested on a
    laptop with local Spark, and the notebooks become thin wrappers that only do
    I/O (read table, call function, write table).

WHERE EACH FUNCTION RUNS (job task order):
    1. dedupe_events          02_silver   drop at-least-once duplicates
    2. parse_changes          02_silver   JSON -> typed rows, tombstones, PII hash
    3. latest_per_key         02_silver   one newest event per key for the MERGE
    4. schema_drift           02_silver   unknown source columns -> ops table
    5. dq_rules_sql           03_quality  flag bad records
    6. dq_catch_rate_sql      03_quality  score rules against ground truth
    7. dim_merchant_scd2_sql  04_gold     SCD2 merchant dimension
    8. fct_payment_lifecycle_sql 04_gold  one row per payment
    9. daily_settlement_sql   04_gold     merchant x day x currency
   10. merchant_risk_sql      04_gold     30-day chargeback/refund rates

WHY GOLD SQL IS BUILT BY FUNCTIONS (fq resolver):
    In Databricks, tables are catalog.schema.table. In local tests they are temp
    views. Every SQL builder takes `fq(schema, table)` so the same SQL runs in
    both places. Gold SQL avoids Databricks-only syntax (QUALIFY, SELECT * EXCEPT)
    for the same reason.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Source table contracts. Must match postgres/init/01_schema.sql.
# Timestamps arrive from Debezium as ISO-8601 strings, so they're STRING here
# and cast afterwards.
# -----------------------------------------------------------------------------
TABLE_SPECS: dict[str, dict] = {
    "merchants": {
        "pk": "merchant_id",
        "schema": "merchant_id BIGINT, name STRING, category STRING, country STRING, "
                  "risk_tier STRING, fee_bps INT, status STRING, created_at STRING, updated_at STRING",
    },
    "customers": {
        "pk": "customer_id",
        "schema": "customer_id BIGINT, email STRING, country STRING, created_at STRING, updated_at STRING",
        # PII is hashed in silver. Raw email never leaves bronze.
        "pii": ["email"],
    },
    "payments": {
        "pk": "payment_id",
        "schema": "payment_id BIGINT, merchant_id BIGINT, customer_id BIGINT, amount_cents BIGINT, "
                  "currency STRING, status STRING, card_brand STRING, failure_reason STRING, "
                  "created_at STRING, updated_at STRING",
    },
    "refunds": {
        "pk": "refund_id",
        "schema": "refund_id BIGINT, payment_id BIGINT, amount_cents BIGINT, reason STRING, "
                  "status STRING, created_at STRING, updated_at STRING",
    },
    "disputes": {
        "pk": "dispute_id",
        "schema": "dispute_id BIGINT, payment_id BIGINT, amount_cents BIGINT, reason STRING, "
                  "status STRING, created_at STRING, updated_at STRING",
    },
}

TS_COLUMNS = ("created_at", "updated_at")
EVENT_ID = ["_kafka_topic", "_kafka_partition", "_kafka_offset"]

# "Is the incoming event newer than what silver already has?"
# Primary order: Postgres WAL position (LSN). Tie-break: Kafka offset (all
# changes to one row share a partition, so offset order = commit order).
# WHY NOT updated_at: two updates in the same millisecond, or a clock change on
# the DB server, would order them wrong. LSN is the database's own ordering.
NEWER = (
    "(s.source_lsn > t.source_lsn) OR "
    "(s.source_lsn = t.source_lsn AND s._kafka_offset > t._kafka_offset)"
)


def expected_columns(table: str) -> list[str]:
    """Column names from the DDL contract, in order."""
    return [c.strip().split()[0] for c in TABLE_SPECS[table]["schema"].split(",")]


def dedupe_events(events: DataFrame) -> DataFrame:
    """
    Drop exact duplicate deliveries.
    The consumer is at-least-once: a crash between writing a file and committing
    Kafka offsets re-delivers the same events. (topic, partition, offset) is a
    globally unique event id, so duplicates are removed exactly, not guessed.
    """
    return events.dropDuplicates(EVENT_ID)


def parse_changes(events: DataFrame, table: str) -> DataFrame:
    """
    Bronze change events (raw JSON) -> typed silver rows for one table.

    - Inserts/updates/snapshots take the `after` image; deletes only have
      `before`, so we coalesce.
    - Deletes become tombstones (_is_deleted = true) with PII nulled out.
    - Unknown JSON keys are ignored here; schema_drift() reports them.
    """
    spec = TABLE_SPECS[table]
    df = (
        events.filter(F.col("table_name") == table)
        .withColumn("_row", F.from_json(F.coalesce(F.col("after"), F.col("before")), spec["schema"]))
        .select(
            *[F.col(f"_row.{c}").alias(c) for c in expected_columns(table)],
            "op", "source_lsn", "source_ts_ms", *EVENT_ID,
        )
    )
    for c in TS_COLUMNS:
        # try_to_timestamp: a malformed value becomes NULL instead of failing the
        # whole batch (serverless runs with ANSI mode, where a bad cast throws).
        df = df.withColumn(c, F.expr(f"try_to_timestamp({c})"))

    df = (
        df.withColumn("_source_ts", F.expr("timestamp_millis(source_ts_ms)"))
        .withColumn("_is_deleted", F.col("op") == "d")
        .withColumn("_processed_at", F.current_timestamp())
    )

    for col in spec.get("pii", []):
        # Hash instead of drop: analysts can still count distinct customers and
        # join on the hash, without seeing emails. Tombstones keep no PII at all.
        df = df.withColumn(
            f"{col}_hash",
            F.when(F.col("_is_deleted"), F.lit(None)).otherwise(
                F.sha2(F.lower(F.trim(F.col(col))), 256)
            ),
        ).drop(col)
    return df


def latest_per_key(df: DataFrame, pk: str) -> DataFrame:
    """
    Keep only the newest event per primary key within a batch.
    MERGE requires at most one source row per target row, and a payment can be
    inserted, captured and refunded inside the same batch.
    """
    w = Window.partitionBy(pk).orderBy(F.col("source_lsn").desc(), F.col("_kafka_offset").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def schema_drift(events: DataFrame, table: str) -> DataFrame:
    """
    Report source columns that the contract doesn't know about.
    WHY DETECT INSTEAD OF AUTO-EVOLVE: silently adding columns to silver hides a
    contract change from downstream users. Detect, alert, and add the column to
    TABLE_SPECS on purpose. Bronze keeps the raw JSON, so nothing is lost while
    we decide, and a replay picks the new column up.
    """
    known = expected_columns(table)
    return (
        events.filter(F.col("table_name") == table)
        .select(F.explode(F.expr("json_object_keys(coalesce(after, before))")).alias("column_name"), "source_lsn")
        .filter(~F.col("column_name").isin(known))
        .groupBy("column_name")
        .agg(F.min("source_lsn").alias("first_seen_lsn"))
        .withColumn("table_name", F.lit(table))
        .withColumn("detected_at", F.current_timestamp())
    )


# =============================================================================
# SQL builders. fq(schema, table) resolves a table name.
# =============================================================================
FQ = Callable[[str, str], str]


def dq_rules_sql(fq: FQ) -> str:
    """
    Data quality rules -> one row per (rule, table, pk) violation.

    WHY FLAG AND EXCLUDE instead of failing the pipeline or dropping rows:
      - failing on one bad payment would block every good one (availability)
      - dropping hides the problem and breaks reconciliation with the source
      - flagging keeps the row (risk team wants to see it) and gold finance
        tables exclude it (finance numbers stay clean)
    """
    p, ph = fq("silver", "payments"), fq("silver", "payments_history")
    r, m = fq("silver", "refunds"), fq("silver", "merchants")
    return f"""
    SELECT 'amount_not_positive' AS rule_name, 'payments' AS table_name,
           CAST(payment_id AS STRING) AS pk, CAST(amount_cents AS STRING) AS observed
    FROM {p} WHERE amount_cents <= 0

    UNION ALL
    SELECT 'invalid_currency', 'payments', CAST(payment_id AS STRING), COALESCE(currency, 'NULL')
    FROM {p} WHERE currency IS NULL OR currency NOT IN ('USD', 'EUR', 'GBP')

    UNION ALL
    -- created_at later than the moment the database committed the insert = bad
    -- client clock or bad code. 5-minute grace for normal clock skew.
    SELECT 'created_in_future', 'payments', CAST(p.payment_id AS STRING), CAST(p.created_at AS STRING)
    FROM {p} p
    JOIN (SELECT payment_id, MIN(_source_ts) AS first_seen FROM {ph} GROUP BY payment_id) f
      ON p.payment_id = f.payment_id
    WHERE p.created_at > f.first_seen + INTERVAL 5 MINUTES

    UNION ALL
    SELECT 'refund_exceeds_payment', 'refunds', CAST(r.refund_id AS STRING),
           CONCAT(CAST(r.amount_cents AS STRING), ' > ', CAST(p.amount_cents AS STRING))
    FROM {r} r JOIN {p} p ON r.payment_id = p.payment_id
    WHERE r.amount_cents > p.amount_cents

    UNION ALL
    SELECT 'refund_on_uncaptured_payment', 'refunds', CAST(r.refund_id AS STRING), p.status
    FROM {r} r JOIN {p} p ON r.payment_id = p.payment_id
    WHERE p.status IN ('authorized', 'failed', 'voided')

    UNION ALL
    SELECT 'missing_country', 'merchants', CAST(merchant_id AS STRING), 'NULL'
    FROM {m} WHERE country IS NULL
    """


def dim_merchant_scd2_sql(fq: FQ) -> str:
    """
    SCD Type 2 merchant dimension, built straight from CDC history.

    WHY BUILD FROM CDC HISTORY instead of dbt-style snapshots: snapshots only see
    the state at snapshot time, so two fee changes between runs collapse into
    one. CDC history has every change with its exact commit time.

    WHY FULL REBUILD instead of incremental SCD2 MERGE: the table is small
    (thousands of rows), a rebuild takes seconds, and it is automatically
    correct when events arrive late or out of order. Incremental SCD2 MERGE is
    where most SCD2 bugs live. Revisit if the dimension reaches millions of rows.

    No-op updates (only updated_at changed) are collapsed so they don't create
    fake versions.

    Steps:
      1. ordered:  every merchant event with the previous event's attributes
      2. versions: keep only events where an attribute really changed
      3. output:   valid_from = change time, valid_to = next change (LEAD)

    Edge case: the FIRST version opens at the merchant's created_at, not at its
    first CDC event. For merchants that existed before the connector, the first
    event is the snapshot read (op 'r'), stamped with the SNAPSHOT time. A
    payment captured before that moment would otherwise match no version, get a
    NULL fee, and silently overstate net settlement.
    """
    h = fq("silver", "merchants_history")
    attrs = ("concat_ws('|', coalesce(name,''), coalesce(category,''), coalesce(country,''), "
             "coalesce(risk_tier,''), coalesce(cast(fee_bps AS STRING),''), coalesce(status,''))")
    return f"""
    WITH ordered AS (
        SELECT merchant_id, name, category, country, risk_tier, fee_bps, status, op,
               created_at, _source_ts, source_lsn, _kafka_offset,
               {attrs} AS attrs,
               LAG({attrs}) OVER (PARTITION BY merchant_id ORDER BY source_lsn, _kafka_offset) AS prev_attrs
        FROM {h}
    ),
    versions AS (
        SELECT * FROM ordered
        WHERE op <> 'd' AND (prev_attrs IS NULL OR prev_attrs <> attrs)
    )
    SELECT merchant_id, name, category, country, risk_tier, fee_bps, status,
           CASE WHEN ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY source_lsn, _kafka_offset) = 1
                THEN LEAST(COALESCE(created_at, _source_ts), _source_ts)
                ELSE _source_ts END AS valid_from,
           COALESCE(LEAD(_source_ts) OVER (PARTITION BY merchant_id ORDER BY source_lsn, _kafka_offset),
                    TIMESTAMP '9999-12-31 00:00:00') AS valid_to,
           LEAD(_source_ts) OVER (PARTITION BY merchant_id ORDER BY source_lsn, _kafka_offset) IS NULL
               AS is_current
    FROM versions
    """


def fct_payment_lifecycle_sql(fq: FQ) -> str:
    """
    One row per payment with every lifecycle timestamp. Only possible because
    CDC kept the intermediate states (nightly snapshots would lose authorized_at
    and captured_at for anything already refunded).
    """
    p, ph = fq("silver", "payments"), fq("silver", "payments_history")
    r, d, dq = fq("silver", "refunds"), fq("silver", "disputes"), fq("ops", "dq_results")
    return f"""
    WITH auth AS (
        SELECT payment_id, MIN(_source_ts) AS authorized_at FROM {ph}
        WHERE op IN ('c', 'r') GROUP BY payment_id
    ),
    cap AS (
        SELECT payment_id, MIN(_source_ts) AS captured_at FROM {ph}
        WHERE status = 'captured' GROUP BY payment_id
    ),
    bad_refunds AS (SELECT CAST(pk AS BIGINT) AS refund_id FROM {dq} WHERE table_name = 'refunds'),
    ref AS (
        SELECT r.payment_id, SUM(r.amount_cents) AS refunded_cents
        FROM {r} r LEFT ANTI JOIN bad_refunds b ON r.refund_id = b.refund_id
        WHERE r.status = 'succeeded' GROUP BY r.payment_id
    ),
    dis AS (
        SELECT payment_id, COUNT(*) AS disputes,
               MAX(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) = 1 AS dispute_lost
        FROM {d} GROUP BY payment_id
    ),
    flagged AS (SELECT DISTINCT CAST(pk AS BIGINT) AS payment_id FROM {dq} WHERE table_name = 'payments')
    SELECT p.payment_id, p.merchant_id, p.customer_id, p.amount_cents, p.currency,
           p.status AS current_status, p.card_brand, p.failure_reason, p.created_at,
           a.authorized_at, c.captured_at,
           COALESCE(ref.refunded_cents, 0) AS refunded_cents,
           COALESCE(dis.disputes, 0) > 0 AS has_dispute,
           COALESCE(dis.dispute_lost, false) AS dispute_lost,
           f.payment_id IS NOT NULL AS is_flagged,
           (unix_timestamp(c.captured_at) - unix_timestamp(a.authorized_at)) AS seconds_to_capture
    FROM {p} p
    LEFT JOIN auth a ON p.payment_id = a.payment_id
    LEFT JOIN cap c ON p.payment_id = c.payment_id
    LEFT JOIN ref ON p.payment_id = ref.payment_id
    LEFT JOIN dis ON p.payment_id = dis.payment_id
    LEFT JOIN flagged f ON p.payment_id = f.payment_id
    """


def daily_settlement_sql(fq: FQ) -> str:
    """
    What each merchant is owed per day, per currency.

    Key decisions:
    - POINT-IN-TIME FEES: fee_bps comes from the SCD2 version valid at capture
      time, not today's fee. Using today's fee would silently restate history
      every time a merchant's pricing changes.
    - PER-CURRENCY, NO FX: summing USD and EUR cents is meaningless. FX
      conversion is out of scope, so every amount is grouped by currency.
    - EVENT-DATE ATTRIBUTION: refunds/chargebacks count on the day they
      settled, not the day of the original payment. A refund arriving 5 days
      late changes that later day, and past days stay closed (finance prefers
      this, and it keeps daily reconciliation stable).
    - FEE ROUNDING PER TRANSACTION, like real processors.
    - Flagged (bad) payments and refunds are excluded.
    """
    lc, dim = fq("gold", "fct_payment_lifecycle"), fq("gold", "dim_merchant_scd2")
    rh, dh, dq = fq("silver", "refunds_history"), fq("silver", "disputes_history"), fq("ops", "dq_results")
    return f"""
    WITH good AS (SELECT * FROM {lc} WHERE NOT is_flagged),
    cap AS (
        SELECT g.merchant_id, g.currency, CAST(g.captured_at AS DATE) AS d,
               1 AS captured_count, g.amount_cents AS gross_cents,
               CAST(ROUND(g.amount_cents * m.fee_bps / 10000.0) AS BIGINT) AS fee_cents,
               0 AS refund_cents, 0 AS dispute_lost_cents
        FROM good g
        LEFT JOIN {dim} m
          ON g.merchant_id = m.merchant_id
         AND g.captured_at >= m.valid_from AND g.captured_at < m.valid_to
        WHERE g.captured_at IS NOT NULL
    ),
    bad_refunds AS (SELECT CAST(pk AS BIGINT) AS refund_id FROM {dq} WHERE table_name = 'refunds'),
    ref AS (
        SELECT g.merchant_id, g.currency, CAST(e.settled_at AS DATE) AS d,
               0, 0, 0, e.amount_cents, 0
        FROM (SELECT refund_id, payment_id, amount_cents, MIN(_source_ts) AS settled_at
              FROM {rh} WHERE status = 'succeeded'
              GROUP BY refund_id, payment_id, amount_cents) e
        LEFT ANTI JOIN bad_refunds b ON e.refund_id = b.refund_id
        JOIN good g ON g.payment_id = e.payment_id
    ),
    dis AS (
        SELECT g.merchant_id, g.currency, CAST(e.lost_at AS DATE) AS d,
               0, 0, 0, 0, e.amount_cents
        FROM (SELECT dispute_id, payment_id, amount_cents, MIN(_source_ts) AS lost_at
              FROM {dh} WHERE status = 'lost'
              GROUP BY dispute_id, payment_id, amount_cents) e
        JOIN good g ON g.payment_id = e.payment_id
    ),
    unioned AS (SELECT * FROM cap UNION ALL SELECT * FROM ref UNION ALL SELECT * FROM dis),
    daily AS (
        SELECT d AS settlement_date, merchant_id, currency,
               SUM(captured_count) AS captured_count,
               SUM(gross_cents) AS gross_cents,
               SUM(fee_cents) AS fee_cents,
               SUM(refund_cents) AS refund_cents,
               SUM(dispute_lost_cents) AS dispute_lost_cents,
               SUM(gross_cents) - SUM(fee_cents) - SUM(refund_cents) - SUM(dispute_lost_cents) AS net_cents
        FROM unioned GROUP BY d, merchant_id, currency
    )
    SELECT *,
           SUM(net_cents) OVER (PARTITION BY merchant_id, currency ORDER BY settlement_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_cents
    FROM daily
    """


def merchant_risk_sql(fq: FQ) -> str:
    """Chargeback and refund rates per merchant over the last 30 days of captures."""
    lc, dim = fq("gold", "fct_payment_lifecycle"), fq("gold", "dim_merchant_scd2")
    return f"""
    WITH win AS (
        SELECT * FROM {lc}
        WHERE captured_at >= (SELECT MAX(captured_at) FROM {lc}) - INTERVAL 30 DAYS
    ),
    agg AS (
        SELECT merchant_id,
               SUM(CASE WHEN captured_at IS NOT NULL THEN 1 ELSE 0 END) AS captured_30d,
               SUM(CASE WHEN has_dispute THEN 1 ELSE 0 END) AS disputes_30d,
               SUM(CASE WHEN dispute_lost THEN 1 ELSE 0 END) AS disputes_lost_30d,
               SUM(CASE WHEN refunded_cents > 0 THEN 1 ELSE 0 END) AS refunded_30d,
               SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged_30d
        FROM win GROUP BY merchant_id
    )
    SELECT a.*, m.name, m.category, m.risk_tier AS current_risk_tier,
           ROUND(a.disputes_30d / NULLIF(a.captured_30d, 0), 4) AS chargeback_rate,
           ROUND(a.refunded_30d / NULLIF(a.captured_30d, 0), 4) AS refund_rate
    FROM agg a LEFT JOIN {dim} m ON a.merchant_id = m.merchant_id AND m.is_current
    """


def dq_catch_rate_sql(fq: FQ) -> str:
    """
    Recall and precision of the quality rules against the simulator's ground
    truth. Only injected records older than the newest processed event count
    (anything newer may simply not have arrived yet).
    """
    inj, dq, ph = fq("ops", "injected_bad_records"), fq("ops", "dq_results"), fq("silver", "payments_history")
    return f"""
    WITH horizon AS (SELECT MAX(_source_ts) - INTERVAL 2 MINUTES AS h FROM {ph}),
    inj AS (
        SELECT DISTINCT i.table_name, CAST(i.pk AS STRING) AS pk, i.kind
        FROM {inj} i CROSS JOIN horizon
        WHERE CAST(i.injected_at AS TIMESTAMP) <= horizon.h
    ),
    flagged AS (SELECT DISTINCT table_name, pk FROM {dq})
    SELECT
        (SELECT COUNT(*) FROM inj) AS injected,
        (SELECT COUNT(*) FROM inj i JOIN flagged f ON i.table_name = f.table_name AND i.pk = f.pk) AS caught,
        (SELECT COUNT(*) FROM flagged f LEFT ANTI JOIN
            (SELECT DISTINCT table_name, CAST(pk AS STRING) AS pk FROM {inj}) i
            ON f.table_name = i.table_name AND f.pk = i.pk) AS false_positives,
        (SELECT COUNT(*) FROM flagged) AS flagged_total
    """
