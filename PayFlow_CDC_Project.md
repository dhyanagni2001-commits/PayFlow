# PayFlow: Real-Time Payments CDC Lakehouse

**Change Data Capture pipeline for a payment processor.** Every insert, update and delete in a Postgres payments database streams through Debezium and Kafka into a Databricks Delta Lake (bronze, silver, gold), is reconciled against the source to the cent, quality-checked against ground truth, orchestrated by Airflow, and served to finance and risk teams in Tableau.

**Stack:** PostgreSQL · Debezium · Apache Kafka · Python · Parquet · Databricks (Delta Lake, Auto Loader, Unity Catalog) · PySpark / Spark SQL · Apache Airflow · Tableau · DuckDB · Docker · GitHub Actions

**Cost:** $0. Everything runs locally in Docker or on free tiers (Databricks Free Edition, Tableau for Students / Tableau Public, GitHub Actions).

---

## Contents

1. [Problem statement](#1-problem-statement)
2. [Success criteria](#2-success-criteria)
3. [Architecture](#3-architecture)
4. [Tech stack: why each tool, what it costs](#4-tech-stack-why-each-tool-what-it-costs)
5. [Data model](#5-data-model)
6. [Repository layout](#6-repository-layout)
7. [Build guide, phase by phase](#7-build-guide-phase-by-phase)
8. [Measuring your resume numbers](#8-measuring-your-resume-numbers)
9. [Resume bullets](#9-resume-bullets)
10. [Interview prep](#10-interview-prep)
11. [Honest limitations](#11-honest-limitations)
12. [Timeline](#12-timeline)
13. [All design decisions in one table](#13-all-design-decisions-in-one-table)
14. [Appendix: full source code, file by file](#appendix-full-source-code-file-by-file)

---

## 1. Problem statement

PayFlow is a (fictional) payment processor. Online merchants use it to accept card payments. Its finance and risk teams currently get data from **nightly full-table exports** of the production Postgres database. That causes three problems:

1. **Stale data.** A fraud spike or a wave of chargebacks shows up a day late.
2. **Lost history.** A payment moves `authorized → captured → refunded → disputed` over days or weeks. A nightly snapshot only keeps the latest state, so questions like "how long do captures take?" or "what fee did this merchant pay on the 15th?" can't be answered.
3. **Numbers don't match.** Re-exports create duplicates, and deleted rows silently linger. Warehouse revenue disagrees with the source ledger, so finance doesn't trust the dashboards.

**Goal:** stream every change from the payments database into a lakehouse within minutes, keep the full history of every row, reconcile with the source to the cent every day, catch bad data before it reaches finance numbers, and give finance and risk a dashboard they trust.

---

## 2. Success criteria

These are the targets. Section 8 explains how to measure each one. **Your resume uses the measured value, never the target.**

| Area | Target | How it's proven |
|---|---|---|
| Completeness | 0 lost changes, including under failures | `verify_no_loss.py` after every chaos test |
| Duplicates | 0 duplicate rows in silver, even after a crash | Event-id dedupe + count check |
| Freshness | Source commit → silver in under ~35 min (bounded by the 30-min schedule) | `ops.pipeline_runs` p50/p95 |
| Correctness | Daily reconciliation matches to the cent | `ops.reconciliation_runs` |
| Data quality | Catch ≥ 95% of injected bad records with 0 false positives | `ops.dq_catch_rate_history` |
| History | Point-in-time merchant fees (SCD Type 2) | `gold.dim_merchant_scd2` + tests |
| Reproducibility | Replay from bronze gives identical gold totals | `payflow_replay` DAG + totals compare |
| Ops | Replication-slot lag and freshness alerting | Airflow tasks fail loudly |

---

## 3. Architecture

```mermaid
flowchart LR
    SIM[Simulator<br/>payments app] -->|SQL| PG[(Postgres<br/>wal_level=logical)]
    PG -->|WAL via<br/>replication slot| DBZ[Debezium<br/>Kafka Connect]
    DBZ -->|1 msg per row change<br/>keyed by PK| K[(Kafka<br/>5 topics x 3 partitions)]
    K --> C[Landing consumer<br/>at-least-once]
    C -->|atomic Parquet files| L[/landing/]
    L -->|Airflow: upload| V[/Databricks Volume/]
    V -->|Auto Loader| B[(bronze.cdc_events)]
    B -->|dedupe + LSN-ordered MERGE| S[(silver: history, state, views)]
    S -->|DQ rules| Q[(ops.dq_results)]
    S --> G[(gold: SCD2, lifecycle,<br/>settlement, risk)]
    Q --> G
    G --> T[Tableau]
    PG -.->|reconcile to the cent| R{{Airflow<br/>reconciliation}}
    S -.-> R
```

```
                         YOUR LAPTOP (Docker)                                 DATABRICKS FREE EDITION (serverless)
┌──────────────────────────────────────────────────────────────┐    ┌──────────────────────────────────────────────┐
│ simulator ─► Postgres ─► Debezium ─► Kafka ─► consumer        │    │ Volume ─► bronze ─► silver ─► gold ─► Tableau │
│                 ▲                               │             │    │                      │                       │
│                 │                          ./landing/*.parquet│───►│                      └─► ops (DQ, health)    │
│   Airflow: upload → run job → freshness/slot checks,          │    │                                               │
│            daily reconciliation, weekly OPTIMIZE, replay      │    │                                               │
└──────────────────────────────────────────────────────────────┘    └──────────────────────────────────────────────┘
```

### Follow one payment through the system

1. The simulator inserts payment #42 with `status = 'authorized'`. Postgres commits and writes the change to its WAL.
2. Debezium reads the WAL through the replication slot and publishes `{op: "c", after: {...}, source.lsn: 2390112}` to topic `payflow.public.payments`, keyed by `payment_id = 42`. All future changes to #42 go to the same partition, so they stay in order.
3. Seconds later the simulator captures it. Debezium publishes `{op: "u", before: {status: authorized}, after: {status: captured}}`. **Nightly ETL would never see the "authorized" state. CDC sees both.**
4. The consumer buffers events, writes `landing/payments/ingest_date=.../part-....parquet` atomically, and only then commits its Kafka offset. A crash in between re-delivers events (duplicates), never loses them.
5. Airflow uploads the file to a Databricks Volume and triggers the job. Auto Loader appends it to `bronze.cdc_events` exactly once.
6. Silver dedupes on the Kafka event id, appends both events to `silver.payments_history`, and MERGEs only the newest (by LSN) into `silver.payments_state`.
7. Quality rules run. Gold rebuilds: #42 gets `authorized_at`, `captured_at`, and a fee computed from the merchant's fee **at capture time** (SCD2).
8. Tableau shows it in the daily settlement. At 00:37 UTC, Airflow checks that yesterday's count and sum in silver equal Postgres to the cent.

---

## 4. Tech stack: why each tool, what it costs

| Layer | Tool | Why this one | Alternative rejected, and why | What we give up |
|---|---|---|---|---|
| Source DB | **PostgreSQL 16** | Most common OLTP DB in job posts; built-in logical decoding (`pgoutput`), no extensions | MySQL: also fine, but Postgres CDC concepts (slots, LSN, publications) are richer to talk about | None significant |
| Change capture | **Debezium 2.7** (Kafka Connect) | Industry standard log-based CDC; reads WAL, so it sees deletes and every intermediate state with zero query load | Polling `updated_at`: misses deletes and intermediate states, and adds load | More moving parts; a stuck slot can fill the source disk (we monitor it) |
| Event log | **Apache Kafka 3.9 (KRaft)** | Durable, replayable, ordered per key; what most DE job posts list. KRaft means no ZooKeeper container | Redpanda: lighter, but less recognizable on a resume (easy swap if RAM is tight) | Single broker = no replication. Fine locally, never in prod |
| Message format | **JSON, no Schema Registry** | Human-readable in Kafka UI, one less container | Avro + Schema Registry: enforced contracts, smaller messages. Right choice with multiple consuming teams | No enforced producer/consumer contract (we detect drift downstream instead) |
| Landing | **Python consumer → Parquet** | Databricks Free Edition (serverless, cloud) can't reach Kafka on a laptop. Files bridge the gap | Databricks reading Kafka directly: the production answer with a cloud Kafka (MSK/Confluent) | Minutes of latency instead of seconds |
| Lakehouse | **Databricks Free Edition** + Delta Lake | Free forever (no 30-day clock like the Snowflake trial). Delta gives ACID MERGE, time travel, OPTIMIZE | Snowflake: great, but the trial expires and the project stops working after a month | Serverless only, daily compute quota, one 2X-Small SQL warehouse, max 5 concurrent job tasks |
| Ingestion | **Auto Loader** (`availableNow`) | Tracks processed files in a checkpoint, so each run only loads new files | `spark.read.parquet(folder)`: re-reads everything, or needs hand-built file tracking | Serverless supports only `availableNow`/`once`, so no always-on stream (fine: Airflow triggers runs) |
| Transform | **PySpark + Spark SQL**, shared `transforms.py` | Logic in pure functions = unit tested locally on plain Spark | dbt-databricks: great for SQL modeling, but CDC MERGE logic with LSN ordering fits PySpark better | Less "analytics engineer" signal than dbt (could add dbt for gold later) |
| Orchestration | **Apache Airflow 2.10** (standalone) | Most-listed orchestrator; Databricks provider has `DatabricksRunNowOperator` | Databricks Workflows schedule alone: simpler, but can't run the local upload, slot check or Postgres reconciliation | One-container Airflow isn't HA (it only coordinates; Databricks does the work) |
| Local analytics | **DuckDB** | SQL over Parquet folders, no server; used for checks and loss verification | Pandas: slower and more code for the same queries | None |
| BI | **Tableau** (Desktop via Tableau for Students, published to Tableau Public) | Requested; most common BI tool in analyst/DE posts | Databricks AI/BI dashboards: free and live, but less recognizable | Tableau Public needs extracts (no live refresh); published workbooks are public (fine, data is synthetic) |
| CI | **GitHub Actions** | Free; runs unit, DAG and real end-to-end (Postgres + Kafka + Debezium) tests on every push | None needed | Databricks isn't exercised in CI (would need secrets + quota); covered by local Spark tests |

---

## 5. Data model

### Source (Postgres, `postgres/init/01_schema.sql`)

| Table | Changes over time | Why it's interesting for CDC |
|---|---|---|
| `merchants` | risk tier, fee (bps) | SCD Type 2 dimension |
| `customers` | inserted, **hard-deleted** (privacy erasure) | Deletes: invisible to polling, visible to CDC |
| `payments` | `authorized → captured → refunded / partially_refunded`, or `→ failed / voided` | Lifecycle history is the whole point |
| `refunds` | `pending → succeeded / failed`, arrive days later | Late-arriving data |
| `disputes` | `open → won / lost`, arrive weeks later | Late-arriving data, risk metrics |

Money is `BIGINT` cents everywhere (exact arithmetic, reconciliation to the cent).

### Lakehouse (Databricks, catalog `payflow`)

| Layer | Table | Grain | Written by | Notes |
|---|---|---|---|---|
| bronze | `cdc_events` | one row per Kafka message | Auto Loader (append) | Raw JSON `before`/`after`, `op`, LSN, Kafka offsets, source file. Never modified. Replay source |
| silver | `<table>_history` | one row per change event, exactly once | MERGE insert-only on `(topic, partition, offset)` | Typed, PII hashed. Input for SCD2 and lifecycle |
| silver | `<table>_state` | one row per primary key | MERGE, only if event is newer by `(lsn, offset)` | Deletes kept as tombstones (`_is_deleted`) |
| silver | `<table>` (view) | current live rows | view over `_state` | What analysts query |
| gold | `dim_merchant_scd2` | one row per merchant version | full rebuild | `valid_from`, `valid_to`, `is_current`; no-op updates collapsed |
| gold | `fct_payment_lifecycle` | one row per payment | full rebuild | authorized/captured timestamps, refunds, disputes, `is_flagged` |
| gold | `daily_merchant_settlement` | merchant × day × currency | full rebuild | gross, point-in-time fee, refunds, chargebacks, net, running balance |
| gold | `merchant_risk_30d` | one row per merchant | full rebuild | chargeback rate, refund rate, flagged count |
| ops | `dq_results`, `dq_catch_rate_history`, `schema_drift_events`, `pipeline_runs`, `reconciliation_runs`, `maintenance_runs`, `watermarks`, `injected_bad_records` | various | notebooks + Airflow | Everything needed to prove the pipeline works |

---

## 6. Repository layout

```
payflow-cdc/
├── docker-compose.yml               Postgres, Kafka, Debezium, Kafka UI, Airflow
├── .env.example                     Databricks host/token/http path, catalog
├── Makefile                         one-word commands (same ones CI uses)
├── requirements.txt / requirements-dev.txt / ruff.toml
├── postgres/init/
│   ├── 01_schema.sql                payments schema
│   ├── 02_cdc_setup.sql             Debezium user + publication
│   └── 03_airflow_db.sql            Airflow metadata database
├── connectors/register_connector.py Debezium config (every setting commented)
├── simulator/simulator.py           payments traffic + bad-data injection + ground truth
├── consumer/consumer.py             Kafka → Parquet, at-least-once, atomic writes
├── uploader/upload_to_volume.py     landing → Databricks Volume → local archive
├── payflow_common/connections.py    Postgres + Databricks SQL helpers
├── databricks/
│   ├── deploy.py                    uploads notebooks, creates/updates the job
│   └── notebooks/
│       ├── transforms.py            ALL transform logic (unit tested)
│       ├── 00_setup.py  01_bronze.py  02_silver.py  03_quality.py  04_gold.py
├── reconciliation/reconcile.py      Postgres vs silver, to the cent
├── airflow/dags/
│   ├── payflow_lakehouse.py         every 30 min: slot check → upload → job → freshness
│   └── payflow_ops.py               daily reconciliation, weekly maintenance, replay
├── scripts/
│   ├── check_landing.py             counts, duplicates, latency, file sizes
│   ├── verify_no_loss.py            Postgres vs landed events, row by row
│   ├── benchmark_throughput.py      max sustained events/sec
│   └── chaos/run_chaos.sh           4 failure scenarios
├── tests/                           consumer, pipeline logic, Spark transforms, DAG integrity
└── .github/workflows/ci.yml         unit, DAG, and real end-to-end CDC test
```

---

## 7. Build guide, phase by phase

Every phase ends with a **Done when** checklist. Don't start the next phase until it's ticked. A working 6-tool pipeline beats a half-working 12-tool one.

### Phase 0: Accounts and machine setup (1 evening)

1. **Docker Desktop**, with **8 GB RAM** assigned (Settings → Resources). Phase 1 needs ~4 GB, Airflow adds ~1.5 GB.
2. **Python 3.11+**, then in the repo: `make install`.
3. **Databricks Free Edition**: sign up at databricks.com (Free Edition, not the trial).
   - Create a catalog named `payflow` (Catalog → + → Create catalog). **If Free Edition won't let you create one**, use the built-in `workspace` catalog and set `PAYFLOW_CATALOG=workspace` in `.env`. All code reads the catalog from that variable.
   - Create a token: Settings → Developer → Access tokens → Generate. If tokens are disabled for your account, run `databricks auth login` (Databricks CLI) instead; the SDK picks up that profile automatically.
   - Get the SQL warehouse's **HTTP path**: SQL Warehouses → your warehouse → Connection details.
4. `cp .env.example .env` and fill it in. `.env` is git-ignored.
5. **Tableau Desktop** via Tableau for Students (free with your .edu email), plus a free **Tableau Public** account. Install the **Databricks ODBC driver** (Tableau prompts you).

**Done when:** `docker info` works, `.env` is filled, you can open the Databricks SQL editor and run `SELECT 1`.

### Phase 1: Capture (Postgres → Debezium → Kafka → Parquet) (1 weekend)

**Files:** `docker-compose.yml`, `postgres/init/01_schema.sql`, `02_cdc_setup.sql`, `connectors/register_connector.py`, `simulator/simulator.py`, `consumer/consumer.py`, `scripts/check_landing.py`, `scripts/verify_no_loss.py`, `tests/test_consumer.py`

```bash
make up            # Postgres, Kafka, Debezium, Kafka UI
make register      # prints "state": "RUNNING" twice (connector + task)
make simulate      # terminal 2
make consume       # terminal 3
# after 5 minutes: Ctrl+C the simulator, wait ~30s, Ctrl+C the consumer
make check         # counts, duplicates, latency
make verify        # zero-loss proof
```

Explore while it runs: Kafka UI at http://localhost:8085 → Topics → `payflow.public.payments` → Messages. Delete a customer in `docker exec -it payflow-postgres psql -U payflow -d payflow` and watch a `d` event appear, plus `u` events on that customer's payments (the foreign key sets `customer_id` to NULL).

**Key tradeoffs**

| Decision | Why | Cost |
|---|---|---|
| `wal_level=logical` + `REPLICA IDENTITY FULL` | Debezium needs logical decoding; FULL gives the complete old row on update/delete (audit trail) | ~2x WAL per update |
| Dedicated `debezium` user, publication created by us | Least privilege; captured tables visible in git | New tables need a manual `ALTER PUBLICATION` |
| Keep the full Debezium envelope (no flattening) | `op`, `before`, LSN drive dedupe, ordering, deletes, SCD2 | Consumer unpacks nested JSON |
| Key = primary key, 3 partitions | Per-row ordering + room for parallel consumers | No global ordering across rows (not needed) |
| At-least-once: write file → rename → commit offset | Crash = duplicates, never loss. Every row has a unique `(topic, partition, offset)` | Dedupe needed downstream |
| Bronze payload as JSON strings | A new source column can't break ingestion | Bronze isn't column-queryable (silver is) |
| Flush on 5,000 events OR 30 s | Bounded latency without thousands of tiny files | Lower flush = lower latency, more small files |
| Live simulator, not a static dataset | CDC needs updates and deletes; ground-truth log of injected bad rows | Synthetic data, so business insights are fictional |

**Done when:** `make verify` prints OK for all 5 tables, `make check` shows 0 dead letters, `pytest tests/test_consumer.py` passes.

### Phase 2: Upload to Databricks (1 evening)

**Files:** `uploader/upload_to_volume.py`, `payflow_common/connections.py`, `databricks/deploy.py`

```bash
make deploy        # uploads notebooks + transforms.py, creates job "payflow-lakehouse"
make upload        # pushes landing/*.parquet to /Volumes/<catalog>/raw/files/landing/
```

Then in Databricks: Workflows → `payflow-lakehouse` → Run now. The first run creates every schema, volume and table (`00_setup` is idempotent).

**Key tradeoffs**

| Decision | Why | Cost |
|---|---|---|
| "Upload then move to archive" (no manifest DB) | The landing folder is the to-do list. A crash re-uploads the same path with `overwrite=True`; Auto Loader skips known paths and silver dedupes anyway | Wasted bandwidth in the crash case |
| Keep a local archive | Can re-upload everything if the Volume is lost | Disk; add a retention policy in prod |
| Job defined in code (`deploy.py`), serverless tasks | Versioned, reproducible, one command for a fresh workspace | None |
| `max_concurrent_runs = 1` | Silver's watermark relies on runs never overlapping | Runs queue instead of overlapping |

**Done when:** the job run is green and `SELECT COUNT(*) FROM payflow.bronze.cdc_events` equals the event count from `make check`.

### Phase 3: Lakehouse: bronze, silver, gold (1 weekend)

**Files:** `databricks/notebooks/transforms.py`, `00_setup.py`, `01_bronze.py`, `02_silver.py`, `04_gold.py`, `tests/test_transforms.py`

How silver applies CDC correctly:

1. **Dedupe** the batch on `(topic, partition, offset)`. Removes at-least-once duplicates exactly.
2. **History**: MERGE insert-only on the event id. Re-running a batch inserts nothing new.
3. **State**: take the newest event per key in the batch (by LSN, then offset), and MERGE it only if it's newer than the stored row.
4. **Deletes are tombstones** (`_is_deleted = true`, PII nulled). Why: an old duplicate "insert" can arrive after the delete. With a hard delete there's nothing to compare it against, and the deleted customer comes back. The tombstone's LSN blocks it.
5. **Watermark** on `_bronze_loaded_at` advances only after all five tables succeed. A failed run is retried by simply running again.

Run the transform tests locally (needs Java 17+):

```bash
.venv/bin/pytest tests/test_transforms.py -v     # 10 tests: dedupe, tombstones, SCD2, point-in-time fees, DQ, catch rate
```

**Key tradeoffs**

| Decision | Why | Cost |
|---|---|---|
| Order by WAL LSN, tie-break Kafka offset | The database's own commit order; immune to clock skew | Requires LSN in every event (Debezium provides it) |
| High-watermark instead of streaming `foreachBatch` | On serverless (Spark Connect), importing a shared module inside `foreachBatch` is fragile. A watermark is plain batch Spark, visible and resettable with SQL | Relies on sequential runs (enforced) |
| Materialize the batch to `ops._silver_batch` | Serverless forbids `df.cache()`; without it each table re-scans bronze | One extra small write per run |
| Hash PII (`email_hash`), null it on tombstones | Analysts can count/join customers without seeing emails | Raw email still exists in bronze JSON (see limitations) |
| Detect schema drift, don't auto-add columns | A silent new column hides a contract change. Bronze keeps the raw data, so a replay picks it up later | Manual step: add the column to `TABLE_SPECS` |
| SCD2 from CDC history, full rebuild | Snapshots miss changes between runs; CDC has every change. Rebuild is seconds and always right with late events | Rebuild cost grows with history (fine below millions of rows) |
| Point-in-time fee join | Today's fee would silently restate past settlements | A range join |
| Gold full rebuild every run | Always correct with late refunds; seconds at this size | At billions of rows: incremental MERGE by affected dates |
| Settlement per currency, no FX | Summing USD and EUR cents is meaningless | No single "total revenue" number across currencies |
| Refunds/chargebacks on the day they settle | Past days stay closed; daily reconciliation stays stable | A late refund lowers a later day, not the payment's day |

**Done when:** all four gold tables have rows, `SELECT COUNT(*), COUNT(DISTINCT _kafka_topic, _kafka_partition, _kafka_offset) FROM payflow.silver.payments_history` returns two equal numbers, and the local transform tests pass.

### Phase 4: Data quality and reconciliation (1 evening)

**Files:** `databricks/notebooks/03_quality.py`, `transforms.dq_rules_sql`, `reconciliation/reconcile.py`, `tests/test_pipeline_logic.py`

| Rule | Table | Catches injected kind |
|---|---|---|
| `amount_not_positive` | payments | `negative_amount`, `zero_amount` |
| `invalid_currency` | payments | `bad_currency` |
| `created_in_future` | payments | `future_timestamp` |
| `refund_exceeds_payment` | refunds | `refund_exceeds_payment` |
| `refund_on_uncaptured_payment` | refunds | `refund_on_failed_payment` |
| `missing_country` | merchants | `null_country` |

Flagged records stay in silver (risk wants to see them) and are **excluded from gold finance tables**. The catch rate compares flags against `simulator/injected/bad_records.jsonl`, which the uploader ships to the Volume.

Reconciliation has two modes, because CDC always trails the source a little:

- **daily** (Airflow, 00:37 UTC): count and sum of cents per created day for payments, refunds, disputes, **closed days only**. Those facts never change after insert, so an exact match is expected even while traffic flows.
- **full** (after chaos tests, simulator stopped): rows and cents per status for every table, plus customer count (proves deletes arrived).

**Done when:** `ops.dq_catch_rate_history` shows your catch rate and false positives, and `make reconcile` exits 0 after stopping the simulator and letting one pipeline run finish.

### Phase 5: Airflow (1 evening)

**Files:** `docker-compose.yml` (airflow service), `postgres/init/03_airflow_db.sql`, `airflow/dags/payflow_lakehouse.py`, `airflow/dags/payflow_ops.py`, `tests/test_dags.py`

```bash
# If Postgres was created in Phase 1, create Airflow's database once:
docker exec -it payflow-postgres psql -U payflow -d payflow -f /docker-entrypoint-initdb.d/03_airflow_db.sql
make airflow-up
docker logs payflow-airflow 2>&1 | grep -i password    # admin password for http://localhost:8080
```

Unpause `payflow_lakehouse`, keep the simulator and consumer running, and watch runs every 30 minutes.

| DAG | Schedule | Tasks |
|---|---|---|
| `payflow_lakehouse` | every 30 min | `check_replication_slot` → `upload_landing_files` (short-circuits if nothing new) → `run_databricks_job` → `check_freshness` |
| `payflow_daily_reconciliation` | 00:37 UTC | `reconcile_closed_days` (fails on any cent mismatch) |
| `payflow_maintenance` | Sunday 03:13 UTC | `optimize_and_vacuum` (records file counts before/after) |
| `payflow_replay` | manual | full refresh of silver/gold from bronze |

**Key tradeoffs**

| Decision | Why | Cost |
|---|---|---|
| Airflow coordinates, Databricks computes | Heavy work in Airflow workers starves the scheduler | Two systems to monitor |
| 30-minute schedule | Free Edition has a daily compute quota; each run starts serverless compute | Freshness is ~30 min, not seconds. Measure the freshness vs. quota curve |
| Short-circuit when no files | No data, no compute spend | None |
| Slot-lag check every run | If Debezium stops, Postgres keeps all WAL and can fill its disk. #1 operational risk of log-based CDC | One cheap query |
| `standalone` Airflow, metadata DB on the same Postgres server | RAM: the official compose is 6+ containers | No HA; orchestrator shares a server with the source DB (separate database) |
| Job looked up by name, not id | No hard-coded ids; redeploys don't break the DAG | Job names must be unique |

**Done when:** three consecutive green `payflow_lakehouse` runs, and `pytest tests/test_dags.py` passes.

### Phase 6: Tableau (1 weekend)

**Connect:** Tableau Desktop → Connect → Databricks. Server hostname = your workspace host (no `https://`), HTTP path = the SQL warehouse's, authentication = Personal Access Token. Use **Extract**, not Live (Tableau Public only accepts extracts, and extracts don't wake the warehouse on every click).

**Calculated fields** (cents → dollars once, reused everywhere):

```
[Gross $]     = SUM([gross_cents]) / 100
[Fees $]      = SUM([fee_cents]) / 100
[Refunds $]   = SUM([refund_cents]) / 100
[Chargebacks $] = SUM([dispute_lost_cents]) / 100
[Net $]       = SUM([net_cents]) / 100
[Take rate]   = SUM([fee_cents]) / SUM([gross_cents])
```

**Dashboard 1: Finance** (source `gold.daily_merchant_settlement`)
- KPI tiles: Gross, Fees, Refunds, Chargebacks, Net, with a **Currency** filter (never mix currencies)
- Line: daily Gross vs Net
- Bar: top 10 merchants by Net
- Line: running balance for a selected merchant (parameter). This is the point-in-time view nightly snapshots can't produce

**Dashboard 2: Risk** (sources `gold.merchant_risk_30d`, `ops.dq_results`)
- Scatter: captured volume (x) vs chargeback rate (y), color = current risk tier
- Bar: flagged records by rule
- Table: merchants above 1% chargeback rate

**Dashboard 3: Pipeline health** (sources `ops.pipeline_runs`, `ops.reconciliation_runs`, `ops.dq_catch_rate_history`, `ops.maintenance_runs`)
- Lines: freshness and latency p50/p95 per run
- Bar: events processed per run
- Tiles: last reconciliation (checks / mismatches), current DQ catch rate, false positives
- Bar: files before vs after OPTIMIZE

**Publish:** File → Save to Tableau Public. Put the link and screenshots in the README. Published workbooks are public. Fine here (synthetic data); never do this with real data.

**Done when:** three dashboards are live on Tableau Public and linked from the README.

### Phase 7: CI (1 evening)

**File:** `.github/workflows/ci.yml`

| Job | What | Why |
|---|---|---|
| `unit` | ruff + consumer/pipeline/Spark transform tests | Fast feedback on logic |
| `dags` | DAG integrity against real Airflow 2.10.5 | A broken DAG file silently disappears from the scheduler |
| `e2e` | Real Postgres + Kafka + Debezium, 60 s of traffic, `verify_no_loss.py` | Catches config bugs (publication, slot, converters) unit tests can't |

**Done when:** a green badge on the README.

### Phase 8: Chaos tests and benchmarks (1 weekend)

```bash
# stack up, connector registered, nothing else running
scripts/chaos/run_chaos.sh kill_consumer         # SIGKILL mid-stream
scripts/chaos/run_chaos.sh crash_before_commit   # crash in the exact duplicate window
scripts/chaos/run_chaos.sh stop_connect          # Debezium down 60 s during writes
scripts/chaos/run_chaos.sh schema_drift          # ALTER TABLE ADD COLUMN mid-stream

# throughput (consumer running in another terminal)
make bench
```

After the chaos runs, upload and run the pipeline, then check in Databricks:

```sql
-- bronze may contain duplicates (expected after crash_before_commit)...
SELECT COUNT(*) - COUNT(DISTINCT _kafka_topic, _kafka_partition, _kafka_offset) AS dup_in_bronze
FROM payflow.bronze.cdc_events;
-- ...silver must have none
SELECT COUNT(*) - COUNT(DISTINCT _kafka_topic, _kafka_partition, _kafka_offset) AS dup_in_silver
FROM payflow.silver.payments_history;
-- schema drift was recorded, pipeline didn't fail
SELECT * FROM payflow.ops.schema_drift_events;
```

Determinism test: note gold totals, trigger `payflow_replay` in Airflow, compare.

```sql
SELECT currency, SUM(gross_cents), SUM(fee_cents), SUM(net_cents)
FROM payflow.gold.daily_merchant_settlement GROUP BY currency;
```

**Done when:** every scenario ends with `verify_no_loss` all OK, 0 duplicates in silver, and identical totals before/after replay. Run each scenario 3 times and report the worst case.

---

## 8. Measuring your resume numbers

**Rule: only numbers you measured go on the resume.** Record each in a "Results" table in the README with the date and machine.

| # | Metric | How to measure | Where it shows | Typical resume phrasing |
|---|---|---|---|---|
| 1 | Total change events processed | `SELECT COUNT(*) FROM payflow.bronze.cdc_events` (let the simulator run for a few days at `--rate 20`) | bronze | "[X]M row-level changes" |
| 2 | Dollars processed | `SELECT SUM(amount_cents)/100 FROM payflow.silver.payments` | silver | "$[X]M in simulated payments" |
| 3 | Sustained throughput | `make bench`: highest rate where `lag@settle` stays near 0 | console | "[X] events/sec sustained" |
| 4 | Capture latency p50/p95 | `make check`, latency block (commit → Parquet) | console | "p95 commit-to-landing latency of [X]s" |
| 5 | End-to-end freshness | `SELECT AVG(latency_p95_s) FROM payflow.ops.pipeline_runs` | ops | "p95 source-to-lakehouse freshness of [X] min" |
| 6 | Zero loss under failure | `run_chaos.sh` × 4 scenarios × 3 runs, `verify_no_loss` | chaos_logs | "0 lost rows across [N] fault-injection runs" |
| 7 | Duplicates removed | `crash_before_commit`: dup_in_bronze vs dup_in_silver | Databricks SQL | "deduplicated [X] redelivered events to 0" |
| 8 | Debezium outage recovery | `stop_connect` summary: WAL held, catch-up seconds | chaos_logs | "recovered from a 60s capture outage in [X]s with 0 loss" |
| 9 | Reconciliation | `SELECT COUNT(*), SUM(mismatches) FROM payflow.ops.reconciliation_runs WHERE mode='daily'` | ops | "reconciled [N] days to the cent with 0 mismatches" |
| 10 | DQ catch rate | latest row of `payflow.ops.dq_catch_rate_history` (caught / injected, false positives) | ops | "caught [X]% of [N] injected bad records, [Y] false positives" |
| 11 | Small-file compaction | `payflow.ops.maintenance_runs` (files_before → files_after) + time a query before/after | ops | "cut file count [X]→[Y], speeding queries [Z]%" |
| 12 | SCD2 history | `SELECT COUNT(*) FROM payflow.gold.dim_merchant_scd2 WHERE NOT is_current` | gold | "tracked [X] merchant pricing/risk changes" |
| 13 | Tests | `pytest --collect-only -q | tail -1` | console | "[N] automated tests incl. end-to-end CDC in CI" |
| 14 | Replay determinism | totals before/after `payflow_replay` | Databricks SQL | "bit-identical gold totals after full replay" |

**Sanity ranges** (if you're far off, something is wrong, investigate before reporting): capture p50 is roughly half the consumer's `--flush-seconds`; end-to-end freshness is dominated by the 30-minute schedule; DQ false positives should be 0 because the simulator's clean data never violates the rules; reconciliation mismatches on closed days should be 0.

---

## 9. Resume bullets

Replace every `[X]` with **your measured value**. Delete a bullet rather than guess a number.

**Project line:**
`PayFlow: Real-Time Payments CDC Lakehouse | Debezium, Kafka, Databricks, Delta Lake, PySpark, Airflow, Tableau`

**Bullets (pick 2-3):**

- Built a change data capture pipeline streaming **[X]M** Postgres row changes (inserts, updates, deletes) through Debezium and Kafka into a Databricks Delta Lake medallion architecture, orchestrated with Airflow and served in Tableau.
- Guaranteed zero data loss with at-least-once delivery, Kafka-offset deduplication, and LSN-ordered Delta MERGEs with delete tombstones; **[N]** fault-injection runs (consumer kills, Debezium outage, schema drift) ended with **0** lost and **0** duplicate rows.
- Reconciled the lakehouse to the source database to the cent across **[N]** days and **$[X]M** of simulated payments, and built an SCD Type 2 merchant dimension for point-in-time fee calculation in daily settlements.
- Wrote data quality rules that caught **[X]%** of **[N]** injected bad records with **[Y]** false positives, and sustained **[X]** events/sec with p95 commit-to-landing latency of **[Y]s** on a single-node stack.

**Short version (ML/SDE resumes, 2 bullets):**

- Built a Debezium + Kafka CDC pipeline into Databricks Delta Lake (Airflow, Tableau) processing **[X]M** row changes with **0** lost or duplicate rows across **[N]** fault-injection tests.
- Reconciled to the source database to the cent across **[N]** days; data quality rules caught **[X]%** of injected bad records with **[Y]** false positives.

---

## 10. Interview prep

**Why CDC instead of a nightly batch export?**
Batch only sees the state at export time. CDC sees every change: deletes (polling can't, the row is gone), intermediate states (authorized before captured), and exact change times. It also puts no query load on the source, since it reads the WAL.

**Is it exactly-once?**
No, and I didn't claim it. The consumer is at-least-once: it writes the file, then commits the Kafka offset, so a crash re-delivers events. Each event carries `(topic, partition, offset)`, a unique id, and silver dedupes on it, so the *result* is exactly-once. Kafka transactions only give true exactly-once when the sink is also Kafka. I proved it with a fault hook that crashes in exactly that window.

**What if events arrive out of order?**
Per row they can't from Kafka (keyed by primary key, one partition). They can after a replay or redelivery. So the silver MERGE only applies an event if its WAL LSN (tie-break: offset) is newer than the stored row. I don't use `updated_at` for ordering because two updates in one millisecond or a clock change would break it.

**How do you handle deletes?**
As tombstones. A hard delete loses the LSN, so an old redelivered insert would bring the row back. The tombstone keeps the key and LSN, with PII nulled, and a view hides it from analysts.

**Biggest operational risk?**
The replication slot. If Debezium stops, Postgres keeps every WAL segment for it and can fill its disk, taking the payments DB down. Airflow checks slot lag every run and fails above 512 MB. I measured the WAL held during a 60-second outage.

**What happens when someone adds a column in Postgres?**
Bronze can't break: it stores raw JSON. Silver detects the unknown key and logs it to `ops.schema_drift_events` without failing. To adopt the column, I add it to the contract and replay from bronze. I chose detect-and-decide over silent auto-evolution so contract changes are visible.

**Why Databricks and not Snowflake?**
Free forever versus a 30-day trial, so the project keeps working. Delta gives ACID MERGE, which CDC needs. Tradeoff: serverless only, daily quota, and the warehouse can't reach my laptop's Kafka, which is why I bridge with Parquet files.

**Why are the files micro-batched instead of streamed?**
Databricks Free Edition runs in the cloud and can't reach a laptop Kafka, and serverless only supports `availableNow` triggers. In production with MSK or Confluent, Databricks would read the topic directly with Structured Streaming and latency would drop from minutes to seconds.

**How does the SCD2 work, and why rebuild it?**
From CDC history: every merchant change has an exact commit time, so `valid_from` is the change time and `valid_to` is the next change (window `LEAD`). No-op updates are collapsed. I rebuild it every run because it's small and a rebuild is automatically correct with late events; incremental SCD2 MERGE is where most SCD2 bugs live.

**How do you reconcile without false alarms when CDC lags?**
Daily mode only compares facts that never change after insert (count, sum of cents per created day) for closed days. Lag can't affect a closed day. Full mode compares everything, including status counts and deletes, after quiescing.

**How would you scale this 100x?**
More partitions and consumer instances (key ordering still holds); Databricks reading Kafka directly; Avro + Schema Registry; incremental gold by affected date partitions instead of full rebuilds; liquid clustering on hot keys; multi-broker Kafka with replication factor 3.

**What about GDPR deletes?**
Silver hashes emails and tombstones carry no PII, but raw emails still exist in bronze JSON and in hashed form in history. True erasure would need bronze retention plus VACUUM, or crypto-shredding (per-customer encryption keys that get deleted). It's listed as a limitation.

---

## 11. Honest limitations

Put these in the README. Interviewers trust a project more when it states its limits.

- **Synthetic data.** Business insights (chargeback rates, top merchants) are fictional. The engineering is real.
- **Single-node everything.** One Kafka broker (replication factor 1), one Airflow container. Numbers are laptop numbers.
- **Micro-batch freshness.** End-to-end freshness is bounded by the 30-minute schedule because of Free Edition constraints.
- **No FX.** Settlement is reported per currency.
- **PII.** Raw emails remain in bronze JSON. See the GDPR answer above.
- **DQ flags are never resolved.** A record fixed later in the source stays flagged.
- **Not tested in CI against Databricks.** Transform logic is covered by local Spark tests; Delta MERGE behavior is only exercised in the workspace.
- **Airflow 2.10.** Airflow 3 is the current line; the DAGs use the TaskFlow API and should port with minor import changes.

---

## 12. Timeline

| Week | Phases | Output |
|---|---|---|
| 1 | 0, 1 | Capture path working, `make verify` all OK |
| 2 | 2, 3 | Bronze/silver/gold in Databricks, transform tests green |
| 3 | 4, 5 | DQ + reconciliation, Airflow running every 30 min |
| 4 | 6, 7 | Tableau dashboards published, CI green |
| 5 | 8 | Chaos + benchmark results table, README, resume bullets filled |

Leave the simulator and pipeline running for several days before measuring metrics 1, 2 and 9. Volume and reconciled days come from elapsed time.

---

## 13. All design decisions in one table

| # | Decision | Chosen | Because | Gave up |
|---|---|---|---|---|
| 1 | Capture method | Log-based CDC (Debezium) | Deletes + every intermediate state, no source load | Slot-management risk |
| 2 | Money type | BIGINT cents | Exact sums; reconcile to the cent | Divide by 100 for display |
| 3 | Source constraints | Few business-rule CHECKs | Realistic messy source; DQ has something to catch | Source accepts bad rows (on purpose) |
| 4 | Replica identity | FULL | Full before-image on update/delete | ~2x WAL per update |
| 5 | CDC permissions | Dedicated user + manual publication | Least privilege | Manual publication updates |
| 6 | Wire format | JSON, no Schema Registry | Readable, fewer containers | No enforced contract |
| 7 | Event envelope | Kept (no flattening) | `op`, `before`, LSN needed | Nested JSON to unpack |
| 8 | Partitioning | Key = PK, 3 partitions | Per-row order, parallelism | No global order |
| 9 | Delivery | At-least-once + event-id dedupe | No loss; exact dedupe | Duplicates in bronze |
| 10 | Bronze payload | JSON strings | Schema drift can't break ingest | Not column-queryable |
| 11 | File writes | tmp + atomic rename | No half-read files | Slightly more code |
| 12 | Offsets | Manual commit after write | Auto-commit can lose data | Commit call per flush |
| 13 | Flush policy | 5,000 events or 30 s | Latency vs small files | Tunable tradeoff |
| 14 | Test data | Live simulator + ground truth | Updates/deletes; measurable DQ | Fictional business numbers |
| 15 | Landing → cloud | Files + upload | Free Edition can't reach local Kafka | Minutes of latency |
| 16 | Upload tracking | Folder as to-do list + archive | Simple, crash-safe | Re-upload on crash |
| 17 | Lakehouse | Databricks Free Edition | Free forever, Delta MERGE | Quota, serverless limits |
| 18 | Ingestion | Auto Loader `availableNow` | Incremental files, serverless-compatible | No always-on stream |
| 19 | Silver incrementality | High-watermark table | Robust on serverless, resettable | Requires sequential runs |
| 20 | Ordering | LSN, then offset | DB commit order, no clock skew | Needs LSN in events |
| 21 | Deletes | Tombstones + view | Prevents resurrection | Tombstone rows kept |
| 22 | PII | SHA-256 hash in silver | Join/count without exposure | Raw PII in bronze |
| 23 | Schema changes | Detect + log, adopt by replay | Visible contract changes | Manual step |
| 24 | SCD2 | From CDC history, full rebuild | Exact change times, correct with late data | Rebuild cost at scale |
| 25 | Gold | Full rebuild per run | Correct with late refunds | Doesn't scale to billions |
| 26 | Fees | Point-in-time from SCD2 | No silent restatement | Range join |
| 27 | Currency | Per-currency, no FX | Correct sums | No cross-currency total |
| 28 | Late events | Attributed to settle day | Closed days stay closed | Not on payment's day |
| 29 | Bad data | Flag + exclude from finance | Availability + clean numbers | Flags never auto-resolve |
| 30 | Reconciliation | Grouped counts/sums, closed days | Cheap, no false alarms | Exactly-cancelling errors |
| 31 | Orchestrator | Airflow coordinates only | Scheduler stays healthy | Two systems |
| 32 | Schedule | 30 min + short-circuit | Fits free quota | ~30 min freshness |
| 33 | Airflow deploy | Standalone, shared PG server | Fits laptop RAM | No HA |
| 34 | BI | Tableau extracts → Public | Free, shareable | No live refresh, public data |
| 35 | Testing | Unit + local Spark + DAG + real e2e CDC in CI | Each layer's bugs caught where they live | Databricks not in CI |

---

## Appendix: full source code, file by file

Create each file at the path shown. Every file is complete. Comments explain the why and the tradeoffs inline.

### Infrastructure

#### `docker-compose.yml`

```yaml
# =============================================================================
# PayFlow CDC - local infrastructure (Phase 1)
#
# Postgres (source OLTP) -> Debezium (Kafka Connect) -> Kafka -> Python consumer
#
# WHY DOCKER COMPOSE:
#   One command spins up the whole stack, anyone can reproduce it, and it costs $0.
#   Tradeoff: single node only, no high availability. That's fine for a portfolio
#   project; in production these would be managed services (RDS, MSK/Confluent).
# =============================================================================

services:

  # ---------------------------------------------------------------------------
  # SOURCE DATABASE: the "production" payments DB we capture changes from.
  # ---------------------------------------------------------------------------
  postgres:
    image: postgres:16
    container_name: payflow-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-payflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-payflow}
      POSTGRES_DB: ${POSTGRES_DB:-payflow}
    # wal_level=logical is REQUIRED for CDC. It makes Postgres write enough detail
    # into the write-ahead log (WAL) for Debezium to rebuild every row change.
    # Tradeoff: slightly more WAL volume and disk I/O than the default "replica".
    # max_replication_slots / max_wal_senders: room for Debezium plus spares.
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=4"
      - "-c"
      - "max_wal_senders=4"
    ports:
      - "5432:5432"
    volumes:
      # Scripts here run once, on first start, in alphabetical order.
      - ./postgres/init:/docker-entrypoint-initdb.d:ro
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-payflow} -d ${POSTGRES_DB:-payflow}"]
      interval: 5s
      timeout: 5s
      retries: 20

  # ---------------------------------------------------------------------------
  # KAFKA: durable, ordered log of change events.
  #
  # WHY KRAFT MODE (no ZooKeeper): KRaft is where Kafka is going (4.x removed
  # ZooKeeper entirely), and it's one less container eating RAM.
  # WHY NOT REDPANDA: Redpanda is lighter, but plain Apache Kafka is what most job
  # descriptions list. Swap to Redpanda if your laptop struggles; Debezium works
  # with either.
  # ---------------------------------------------------------------------------
  kafka:
    image: apache/kafka:3.9.1
    container_name: payflow-kafka
    ports:
      - "29092:29092"   # for clients on your laptop (simulator/consumer)
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # Two listeners: one for containers (kafka:9092), one for your laptop
      # (localhost:29092). Without this split, clients on the host get told to
      # connect to "kafka:9092", which doesn't resolve outside Docker.
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,PLAINTEXT_HOST://:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      # Single broker, so every replication factor must be 1.
      # Tradeoff: losing this container loses data. Production uses 3.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      # 3 partitions per topic so we can later show parallel consumers.
      # Debezium keys each message by the row's primary key, so all changes to
      # one payment always land in the same partition = per-payment ordering.
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      # Keep 7 days of events so we can replay/backfill after a bug.
      KAFKA_LOG_RETENTION_HOURS: 168
    # No volume on purpose: Kafka data lives inside the container and survives
    # `docker compose stop/start`, but is wiped by `docker compose down`.
    # Tradeoff: simpler (no file-permission issues with the non-root Kafka image)
    # at the cost of losing events on `down`. The source of truth is Postgres, so
    # Debezium can always re-snapshot.
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 20

  # ---------------------------------------------------------------------------
  # DEBEZIUM (runs inside Kafka Connect): reads the Postgres WAL and publishes
  # one Kafka message per row change (insert/update/delete).
  #
  # WHY LOG-BASED CDC instead of polling "WHERE updated_at > last_run":
  #   + catches DELETEs (polling can't see a row that's gone)
  #   + catches every intermediate state, not just the latest one
  #   + no extra load from repeated full-table queries
  #   - more moving parts, and a stuck replication slot can fill the source disk
  #     (we guard against that with heartbeats + monitoring later)
  # ---------------------------------------------------------------------------
  connect:
    image: quay.io/debezium/connect:2.7
    container_name: payflow-connect
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    ports:
      - "8083:8083"   # Kafka Connect REST API (register/inspect connectors)
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: payflow-connect
      # Connect stores its own config, source offsets (= last WAL position read)
      # and status in Kafka topics. This is what lets Debezium resume exactly
      # where it stopped after a restart.
      CONFIG_STORAGE_TOPIC: _connect_configs
      OFFSET_STORAGE_TOPIC: _connect_offsets
      STATUS_STORAGE_TOPIC: _connect_statuses
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD-SHELL", "curl -fs http://localhost:8083/connectors >/dev/null"]
      interval: 10s
      timeout: 5s
      retries: 30

  # ---------------------------------------------------------------------------
  # KAFKA UI: browse topics and messages at http://localhost:8085
  # Pure convenience for debugging. Not part of the pipeline.
  # (Port 8085 because Airflow will take 8080 in Phase 3.)
  # ---------------------------------------------------------------------------
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: payflow-kafka-ui
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8085:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: payflow
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: debezium
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://connect:8083

  # ---------------------------------------------------------------------------
  # AIRFLOW (Phase 5): orchestrates upload -> Databricks job -> checks,
  # daily reconciliation, maintenance, and replays.
  #
  # WHY `standalone` (one container: scheduler + webserver + triggerer):
  #   The official compose file runs 6+ containers (~4 GB RAM). On top of
  #   Kafka + Debezium + Postgres, that doesn't fit a laptop. Standalone with
  #   LocalExecutor keeps real parallelism in one container.
  #   Tradeoff: no HA, not how production runs. Fine because Airflow here only
  #   *coordinates*; the heavy work runs in Databricks.
  # WHY METADATA IN THE SAME POSTGRES SERVER (separate `airflow` database):
  #   saves another ~300 MB container. Production would use its own instance
  #   so orchestrator load can never touch the payments database.
  # WHY _PIP_ADDITIONAL_REQUIREMENTS: zero build step for a portfolio project.
  #   Tradeoff: slower first start. Production bakes a custom image.
  # ---------------------------------------------------------------------------
  airflow:
    image: apache/airflow:2.10.5-python3.11
    container_name: payflow-airflow
    depends_on:
      postgres:
        condition: service_healthy
    command: standalone
    user: "${AIRFLOW_UID:-50000}:0"      # Linux: set AIRFLOW_UID=$(id -u) in .env
    ports:
      - "8080:8080"
    env_file:
      - path: .env
        required: false     # Phase 1 runs without a .env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
      _PIP_ADDITIONAL_REQUIREMENTS: >-
        apache-airflow-providers-databricks>=6.5
        databricks-sdk databricks-sql-connector psycopg[binary]>=3.2
      # Airflow connection used by DatabricksRunNowOperator, built from .env.
      AIRFLOW_CONN_DATABRICKS_DEFAULT: '{"conn_type": "databricks", "host": "${DATABRICKS_HOST}", "password": "${DATABRICKS_TOKEN}"}'
      # Inside Docker, Postgres is reached by service name, not localhost.
      PG_DSN: postgresql://payflow:payflow@postgres:5432/payflow
      PYTHONPATH: /opt/payflow
      LANDING_DIR: /opt/payflow/landing
      ARCHIVE_DIR: /opt/payflow/landing_archive
      GROUND_TRUTH: /opt/payflow/simulator/injected/bad_records.jsonl
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./:/opt/payflow                  # uploader, reconciliation, landing files

volumes:
  pg_data:
```

#### `.env.example`

```bash
# Copy to .env and fill in. .env is git-ignored. Never commit tokens.

# Databricks Free Edition workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com
DATABRICKS_HOST=
# Settings -> Developer -> Access tokens -> Generate new token
DATABRICKS_TOKEN=
# SQL Warehouses -> your warehouse -> Connection details -> HTTP path
DATABRICKS_HTTP_PATH=
# Use "workspace" if Free Edition doesn't let you create a catalog named payflow
PAYFLOW_CATALOG=payflow

# Linux only: your user id, so Airflow can move files in ./landing
AIRFLOW_UID=50000
```

#### `requirements.txt`

```text
# Runtime dependencies (laptop + Airflow container)
psycopg[binary]>=3.2         # Postgres driver: simulator, reconciliation, slot-lag check
faker>=30.0                  # fake names/emails for the simulator
confluent-kafka>=2.5         # Kafka consumer (librdkafka: faster/more reliable than kafka-python)
pyarrow>=17.0                # Parquet writer
duckdb>=1.1                  # local SQL over Parquet: checks, loss verification
databricks-sdk>=0.40         # Volume uploads, notebook/job deployment
databricks-sql-connector>=3.4  # SQL warehouse queries: reconciliation, freshness, maintenance
```

#### `requirements-dev.txt`

```text
# Test/lint only
pytest>=8.0
ruff>=0.6
pyspark==3.5.3   # local tests of databricks/notebooks/transforms.py (needs Java 17+)
```

#### `ruff.toml`

```toml
line-length = 120
target-version = "py311"

[lint]
select = ["E", "F", "I", "B"]
ignore = ["E501"]   # long SQL strings are clearer unwrapped

[lint.per-file-ignores]
# Databricks notebooks: spark, dbutils, display are injected by the runtime.
"databricks/notebooks/0*.py" = ["F821", "E402", "I001"]
"tests/*" = ["E402"]
```

#### `Makefile`

```makefile
# Shortcuts. GitHub Actions calls the same commands, so local == CI.
.PHONY: install up register simulate consume check verify upload deploy reconcile test chaos bench airflow-up down reset

install:          ## venv + all Python deps
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt

up:               ## Postgres, Kafka, Debezium, Kafka UI (Phase 1 stack)
	docker compose up -d --wait postgres kafka connect kafka-ui

register:         ## register Debezium connector (idempotent)
	.venv/bin/python connectors/register_connector.py

simulate:         ## payments traffic (Ctrl+C to stop)
	.venv/bin/python simulator/simulator.py --rate 20 --bad-rate 0.02

consume:          ## Kafka -> Parquet landing (Ctrl+C to stop)
	.venv/bin/python consumer/consumer.py

check:            ## landing metrics: counts, duplicates, latency, file sizes
	.venv/bin/python scripts/check_landing.py

verify:           ## prove zero loss: Postgres vs landed events (stop simulator first)
	.venv/bin/python scripts/verify_no_loss.py

deploy:           ## push notebooks + create/update the Databricks job
	set -a && . ./.env && set +a && .venv/bin/python databricks/deploy.py

upload:           ## manual upload of landing files to the Databricks Volume
	set -a && . ./.env && set +a && .venv/bin/python uploader/upload_to_volume.py

reconcile:        ## quiesced full reconciliation (stop simulator, wait for a pipeline run)
	set -a && . ./.env && set +a && .venv/bin/python reconciliation/reconcile.py --mode full

airflow-up:       ## start Airflow (http://localhost:8080, password in container logs)
	docker compose up -d airflow

test:             ## lint + all local tests
	.venv/bin/ruff check . && .venv/bin/pytest tests -v

chaos:            ## all chaos scenarios
	for s in kill_consumer crash_before_commit stop_connect schema_drift; do scripts/chaos/run_chaos.sh $$s; done

bench:            ## throughput benchmark (consumer must be running)
	.venv/bin/python scripts/benchmark_throughput.py --rates 50 100 200 400 --procs 4

down:             ## stop containers (keeps Postgres data)
	docker compose down

reset:            ## wipe everything local: containers, volumes, landing files
	docker compose down -v && rm -rf landing landing_archive simulator/injected chaos_logs
```

#### `.gitignore`

```gitignore
# Python
__pycache__/
*.pyc
.venv/

# Generated data: never commit (large, and reproducible by re-running)
landing/
simulator/injected/

# Secrets
.env
landing_archive/
chaos_logs/
```

### Phase 1: Source database

#### `postgres/init/01_schema.sql`

```sql
-- =============================================================================
-- PayFlow source schema: the "production" OLTP database we capture changes from.
--
-- Design notes (each one is a deliberate tradeoff):
--
-- 1. MONEY AS BIGINT CENTS, not NUMERIC/FLOAT.
--    Floats can't represent 0.10 exactly, so sums drift and reconciliation fails.
--    NUMERIC works in Postgres, but Debezium encodes it as base64 bytes by
--    default, which is painful downstream. Integer cents are exact AND simple.
--    Tradeoff: every consumer must remember to divide by 100 for display.
--
-- 2. FEW CHECK CONSTRAINTS ON PURPOSE.
--    Real source systems are messy: legacy code, bugs, manual fixes. We keep
--    foreign keys (structural integrity) but leave business rules like
--    "refund <= payment amount" unenforced, so the simulator can inject bad data
--    that our downstream quality checks must catch. That's the realistic setup:
--    the warehouse can't assume the source is clean.
--
-- 3. updated_at MAINTAINED BY TRIGGER.
--    Log-based CDC doesn't need it, but analysts do, and it lets us compare
--    "time of change in source" vs "time event reached the lakehouse" = latency.
-- =============================================================================

-- Merchants: businesses using PayFlow to accept payments.
-- risk_tier and fee_bps change over time -> this becomes an SCD Type 2 dimension.
CREATE TABLE merchants (
    merchant_id     BIGSERIAL PRIMARY KEY,
    name            TEXT        NOT NULL,
    category        TEXT        NOT NULL,             -- e.g. 'electronics', 'travel'
    country         CHAR(2),                          -- nullable: bad-data target
    risk_tier       TEXT        NOT NULL DEFAULT 'low',
    fee_bps         INTEGER     NOT NULL DEFAULT 290, -- 2.90% processing fee
    status          TEXT        NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Customers: cardholders paying merchants.
-- Can be hard-deleted (simulating a privacy/GDPR erasure request). Polling-based
-- ETL would never notice those deletes; CDC does.
CREATE TABLE customers (
    customer_id     BIGSERIAL PRIMARY KEY,
    email           TEXT        NOT NULL,
    country         CHAR(2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Payments: the core fact. A payment moves through a lifecycle:
--   authorized -> captured -> (partially_)refunded
--   authorized -> voided
--   authorized -> failed
-- Each status change is an UPDATE. Nightly snapshots would only see the final
-- state; CDC sees every transition, which is the whole point of this project.
CREATE TABLE payments (
    payment_id      BIGSERIAL PRIMARY KEY,
    merchant_id     BIGINT      NOT NULL REFERENCES merchants(merchant_id),
    customer_id     BIGINT      REFERENCES customers(customer_id) ON DELETE SET NULL,
    amount_cents    BIGINT      NOT NULL,   -- no CHECK (> 0) on purpose, see note 2
    currency        TEXT        NOT NULL,   -- TEXT not CHAR(3) so bad values can land
    status          TEXT        NOT NULL,
    card_brand      TEXT,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Refunds: money going back to the customer. Can arrive days after capture,
-- which is a classic "late-arriving data" problem for daily revenue numbers.
CREATE TABLE refunds (
    refund_id       BIGSERIAL PRIMARY KEY,
    payment_id      BIGINT      NOT NULL REFERENCES payments(payment_id),
    amount_cents    BIGINT      NOT NULL,
    reason          TEXT,
    status          TEXT        NOT NULL DEFAULT 'pending', -- pending -> succeeded/failed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Disputes (chargebacks): customer's bank pulls money back. Arrive weeks later
-- and go through their own lifecycle: open -> won / lost.
CREATE TABLE disputes (
    dispute_id      BIGSERIAL PRIMARY KEY,
    payment_id      BIGINT      NOT NULL REFERENCES payments(payment_id),
    amount_cents    BIGINT      NOT NULL,
    reason          TEXT        NOT NULL,   -- 'fraudulent', 'product_not_received', ...
    status          TEXT        NOT NULL DEFAULT 'open',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes the simulator needs (it looks up payments by status).
CREATE INDEX idx_payments_status   ON payments(status);
CREATE INDEX idx_payments_merchant ON payments(merchant_id);
CREATE INDEX idx_refunds_payment   ON refunds(payment_id);
CREATE INDEX idx_disputes_payment  ON disputes(payment_id);

-- -----------------------------------------------------------------------------
-- updated_at trigger
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_merchants_updated BEFORE UPDATE ON merchants FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_customers_updated BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_payments_updated  BEFORE UPDATE ON payments  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_refunds_updated   BEFORE UPDATE ON refunds   FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_disputes_updated  BEFORE UPDATE ON disputes  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- -----------------------------------------------------------------------------
-- REPLICA IDENTITY FULL
-- By default, UPDATE/DELETE events only include the primary key in the "before"
-- image. FULL makes Postgres log the entire old row.
--   + we can see exactly what changed (e.g. status went captured -> refunded)
--   + deletes carry the full deleted row, useful for audit
--   - more WAL written per update (roughly 2x row size)
-- For a payments audit trail, the visibility is worth the extra WAL.
-- -----------------------------------------------------------------------------
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE payments  REPLICA IDENTITY FULL;
ALTER TABLE refunds   REPLICA IDENTITY FULL;
ALTER TABLE disputes  REPLICA IDENTITY FULL;
```

#### `postgres/init/02_cdc_setup.sql`

```sql
-- =============================================================================
-- CDC setup: a dedicated least-privilege user for Debezium + an explicit
-- publication listing exactly which tables get captured.
--
-- WHY A SEPARATE USER instead of reusing the app/superuser:
--   If the connector config leaks, the blast radius is "can read 5 tables",
--   not "can drop the database". Also how any real DBA would set it up.
--
-- WHY CREATE THE PUBLICATION OURSELVES (and tell Debezium not to):
--   If Debezium auto-creates it, its user needs table-owner/superuser rights.
--   Creating it here keeps Debezium's user minimal, and the list of captured
--   tables is visible and version-controlled.
--   Tradeoff: adding a new table means altering the publication by hand.
-- =============================================================================

-- NOTE: plain-text password is fine for local Docker only. In a real setup this
-- would come from a secrets manager.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';

GRANT CONNECT ON DATABASE payflow TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
-- SELECT is needed for the initial snapshot (Debezium reads existing rows once
-- before switching to streaming the WAL).
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- pgoutput is Postgres's built-in logical decoding plugin. No extensions to
-- install, works on RDS/Cloud SQL too. (The older wal2json/decoderbufs plugins
-- need extra installs.)
CREATE PUBLICATION payflow_pub FOR TABLE
    merchants, customers, payments, refunds, disputes;
```

#### `postgres/init/03_airflow_db.sql`

```sql
-- =============================================================================
-- Separate database + user for Airflow's metadata (Phase 5).
-- Lives on the same Postgres server to save RAM, but in its own database, and
-- it's NOT in the payflow_pub publication, so Debezium never captures it.
--
-- NOTE: init scripts run only on an EMPTY data volume. If you already ran
-- Phase 1, either `make reset` or run these two statements by hand:
--   docker exec -it payflow-postgres psql -U payflow -d payflow -f /docker-entrypoint-initdb.d/03_airflow_db.sql
-- =============================================================================
CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
```

### Phase 1: Capture

#### `connectors/register_connector.py`

```python
"""
Register (or update) the Debezium Postgres connector with Kafka Connect.

Run:  python connectors/register_connector.py
Check: curl -s localhost:8083/connectors/payflow-postgres/status

WHY PYTHON INSTEAD OF A JSON FILE + curl:
    JSON can't hold comments, and every setting below is a decision worth
    explaining. Uses only the standard library, so no extra installs.

WHY PUT /connectors/<name>/config INSTEAD OF POST /connectors:
    PUT is idempotent: it creates the connector if missing and updates it if
    present. POST fails if it already exists. Idempotent setup scripts can be
    re-run safely (and later called from Airflow or CI).
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "payflow-postgres"

CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    # --- Source connection --------------------------------------------------
    # "postgres" is the Docker service name; Connect runs inside the Docker
    # network, so it doesn't use localhost.
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",          # least-privilege user from 02_cdc_setup.sql
    "database.password": "debezium",
    "database.dbname": "payflow",

    # --- Logical decoding ---------------------------------------------------
    # pgoutput = built into Postgres, nothing to install.
    "plugin.name": "pgoutput",
    # We created the publication ourselves (see 02_cdc_setup.sql), so Debezium
    # must not try to create/alter it.
    "publication.name": "payflow_pub",
    "publication.autocreate.mode": "disabled",
    # The replication slot is Postgres's bookmark of how far Debezium has read.
    # Postgres keeps WAL until the slot confirms it. RISK: if the connector is
    # down for days, WAL piles up and can fill the source disk. Phase 4 adds a
    # monitor on slot lag for exactly this reason.
    "slot.name": "payflow_slot",

    # --- What to capture ----------------------------------------------------
    # Topic names become: payflow.public.payments, payflow.public.refunds, ...
    "topic.prefix": "payflow",
    "table.include.list": (
        "public.merchants,public.customers,public.payments,"
        "public.refunds,public.disputes"
    ),

    # --- Snapshot -----------------------------------------------------------
    # "initial": on first start, read every existing row once (op = "r"), then
    # stream changes. Without it, the lakehouse would miss rows created before
    # the connector existed.
    "snapshot.mode": "initial",

    # --- Message format -----------------------------------------------------
    # JSON without embedded schemas.
    #   + human-readable in Kafka UI, no Schema Registry container to run
    #   - bigger messages; no enforced schema contract between producer and
    #     consumer (Avro + Schema Registry gives you that)
    # For a single-team portfolio project, readability wins. Mention in
    # interviews that production would likely use Avro/Protobuf + a registry.
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    # We KEEP the full Debezium envelope (before, after, op, source.lsn, ts_ms)
    # instead of flattening with the ExtractNewRecordState transform.
    #   + "before" shows what changed; "op" tells insert/update/delete;
    #     "source.lsn" gives a strict per-row order for dedupe and SCD2
    #   - consumer has to unpack it (a few lines of code)
    # Flattening would throw away exactly the metadata a CDC pipeline needs.

    # Tombstones are extra null-value messages after deletes, used for Kafka log
    # compaction. Our topics use time-based retention, not compaction, and the
    # delete event already carries op="d", so tombstones would just be noise.
    "tombstones.on.delete": "false",

    # Timestamps arrive as ISO-8601 strings (TIMESTAMPTZ default), and we store
    # money as BIGINT, so no special decimal handling is needed. Set anyway so a
    # future NUMERIC column comes through as a readable string, not base64.
    "decimal.handling.mode": "string",

    # --- Health -------------------------------------------------------------
    # Heartbeats let Debezium confirm progress to Postgres even when the captured
    # tables are quiet, so the replication slot doesn't hold WAL forever.
    "heartbeat.interval.ms": "10000",
}


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict | list | str]:
    """Tiny HTTP helper around urllib (avoids adding `requests` as a dependency)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else ""
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def wait_for_connect(timeout_s: int = 120) -> None:
    """Kafka Connect takes a while to boot; poll until its REST API answers."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            status, _ = request("GET", "/connectors")
            if status == 200:
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        print("Waiting for Kafka Connect...")
        time.sleep(5)
    sys.exit(f"Kafka Connect not reachable at {CONNECT_URL} after {timeout_s}s")


def main() -> None:
    wait_for_connect()

    status, body = request("PUT", f"/connectors/{CONNECTOR_NAME}/config", CONFIG)
    if status not in (200, 201):
        sys.exit(f"Failed to register connector ({status}): {body}")
    print(f"Connector '{CONNECTOR_NAME}' registered (HTTP {status}).")

    # Give it a moment to start, then report task state. A FAILED task usually
    # means a config/permission problem; the trace explains why.
    time.sleep(5)
    _, state = request("GET", f"/connectors/{CONNECTOR_NAME}/status")
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
```

#### `simulator/simulator.py`

```python
"""
PayFlow traffic simulator: plays the role of the production payments app.

It keeps creating, updating and deleting rows in Postgres so Debezium has a
realistic stream of changes to capture, and it deliberately injects bad data
(at --bad-rate) so the downstream quality checks have something real to catch.

Run:
    python simulator/simulator.py --rate 20 --bad-rate 0.02
    python simulator/simulator.py --duration 300 --seed 42   # 5-minute reproducible run

WHY A LIVE SIMULATOR instead of loading a static dataset (PaySim, Kaggle):
    CDC is about *changes*. A static CSV gives you inserts only: no status
    transitions, no late refunds, no deletes. A simulator produces the whole
    lifecycle, at a rate you control, forever. Tradeoff: the data is synthetic,
    so business insights from it are made up. That's fine: the project is about
    the pipeline, not the insights. (You could seed initial rows from PaySim
    later if you want more realistic amount distributions.)

WHY GROUND-TRUTH LOGGING of injected bad records:
    Every bad row we inject is written to simulator/injected/bad_records.jsonl.
    Later we join that against what the quality checks flagged, which gives a
    real "caught X% of bad records" number for the resume, instead of a guess.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from faker import Faker

PG_DSN = os.getenv("PG_DSN", "postgresql://payflow:payflow@localhost:5432/payflow")
BAD_LOG = Path(__file__).parent / "injected" / "bad_records.jsonl"

# --- Reference values -------------------------------------------------------
CATEGORIES = ["electronics", "travel", "food_delivery", "apparel", "saas", "gaming", "health"]
COUNTRIES = ["US", "US", "US", "GB", "DE", "IN", "CA", "FR", "BR"]  # repeated = weighted
CURRENCIES = ["USD"] * 8 + ["EUR", "GBP"]
CARD_BRANDS = ["visa", "visa", "mastercard", "amex", "discover"]
FAILURE_REASONS = ["insufficient_funds", "card_declined", "expired_card", "fraud_suspected"]
REFUND_REASONS = ["requested_by_customer", "duplicate", "fraudulent"]
DISPUTE_REASONS = ["fraudulent", "product_not_received", "unrecognized", "duplicate"]
RISK_TIERS = ["low", "medium", "high"]

# --- Action mix -------------------------------------------------------------
# Weights roughly mirror a real processor: lots of new payments and captures,
# a trickle of refunds, rare disputes and merchant changes.
ACTIONS = {
    "new_payment":     45,
    "capture":         25,
    "fail_or_void":     5,
    "new_refund":       6,
    "settle_refund":    5,
    "open_dispute":     1.5,
    "resolve_dispute":  1.5,
    "new_customer":     6,
    "update_merchant":  2,   # drives SCD Type 2 history downstream
    "new_merchant":     0.5,
    "delete_customer":  0.5, # privacy erasure; CDC sees deletes, polling can't
}


class Simulator:
    def __init__(self, conn: psycopg.Connection, bad_rate: float, seed: int | None):
        self.conn = conn
        self.bad_rate = bad_rate
        self.rng = random.Random(seed)
        self.fake = Faker()
        if seed is not None:
            Faker.seed(seed)
        self.stats: Counter[str] = Counter()
        BAD_LOG.parent.mkdir(parents=True, exist_ok=True)
        self.bad_log = BAD_LOG.open("a", encoding="utf-8")

    # ------------------------------------------------------------------ helpers
    def is_bad(self) -> bool:
        return self.rng.random() < self.bad_rate

    def log_bad(self, table: str, pk: int, kind: str) -> None:
        """Record ground truth for every injected bad row."""
        self.bad_log.write(json.dumps({
            "injected_at": datetime.now(timezone.utc).isoformat(),
            "table": table, "pk": pk, "kind": kind,
        }) + "\n")
        self.bad_log.flush()
        self.stats[f"bad:{kind}"] += 1

    def amount_cents(self) -> int:
        # Log-normal: most payments are small, a long tail is large. Median ~$40.
        return max(50, int(self.rng.lognormvariate(8.3, 1.0)))

    def max_id(self, cur: psycopg.Cursor, table: str, pk: str) -> int:
        cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM {table}")
        return cur.fetchone()[0]

    def pick_recent(self, cur, table: str, pk: str, where: str, window: int = 5000):
        """
        Pick a random-ish recent row matching `where`.

        WHY NOT `ORDER BY random() LIMIT 1`: that sorts every matching row, which
        gets slow as the table grows (the simulator would slow itself down).
        Instead: jump to a random id inside the most recent `window` ids and take
        the first match at or after it. Uses the primary-key index, so it stays
        fast at millions of rows. Tradeoff: not perfectly uniform. Doesn't matter
        for a simulator.
        """
        top = self.max_id(cur, table, pk)
        if top == 0:
            return None
        start = self.rng.randint(max(1, top - window), top)
        cur.execute(
            f"SELECT * FROM {table} WHERE {pk} >= %s AND {where} ORDER BY {pk} LIMIT 1",
            (start,),
        )
        row = cur.fetchone()
        if row is None:  # nothing after `start`; try before it
            cur.execute(
                f"SELECT * FROM {table} WHERE {pk} < %s AND {where} ORDER BY {pk} DESC LIMIT 1",
                (start,),
            )
            row = cur.fetchone()
        return row

    # ----------------------------------------------------------------- bootstrap
    def bootstrap(self, n_merchants: int, n_customers: int) -> None:
        """Create the initial merchants/customers once (skipped if they exist)."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM merchants")
            if cur.fetchone()[0] >= n_merchants:
                print("Bootstrap skipped: data already present.")
                return
            print(f"Bootstrapping {n_merchants} merchants, {n_customers} customers...")
            cur.executemany(
                "INSERT INTO merchants (name, category, country, risk_tier, fee_bps) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(self.fake.company(), self.rng.choice(CATEGORIES), self.rng.choice(COUNTRIES),
                  self.rng.choices(RISK_TIERS, weights=[80, 15, 5])[0],
                  self.rng.choice([250, 290, 290, 320]))
                 for _ in range(n_merchants)],
            )
            cur.executemany(
                "INSERT INTO customers (email, country) VALUES (%s, %s)",
                [(self.fake.unique.email(), self.rng.choice(COUNTRIES)) for _ in range(n_customers)],
            )
        self.conn.commit()

    # ------------------------------------------------------------------- actions
    # Each action = one small transaction, like a real app request.
    # WHY ONE TRANSACTION PER ACTION: it matches how an OLTP app behaves and
    # gives Debezium realistic, small transactions. Tradeoff: lower write
    # throughput than batching, but we're simulating an app, not bulk-loading.

    def new_payment(self, cur) -> None:
        merchant = self.pick_recent(cur, "merchants", "merchant_id", "status = 'active'", window=10**9)
        customer = self.pick_recent(cur, "customers", "customer_id", "TRUE", window=10**9)
        if not merchant or not customer:
            return
        amount, currency, created_at, bad_kind = self.amount_cents(), self.rng.choice(CURRENCIES), None, None

        if self.is_bad():
            bad_kind = self.rng.choice(["negative_amount", "zero_amount", "bad_currency", "future_timestamp"])
            if bad_kind == "negative_amount":
                amount = -amount
            elif bad_kind == "zero_amount":
                amount = 0
            elif bad_kind == "bad_currency":
                currency = self.rng.choice(["usd ", "US$", "", "XXX"])
            elif bad_kind == "future_timestamp":
                created_at = datetime.now(timezone.utc) + timedelta(days=self.rng.randint(1, 30))

        cur.execute(
            "INSERT INTO payments (merchant_id, customer_id, amount_cents, currency, status, card_brand, created_at) "
            "VALUES (%s, %s, %s, %s, 'authorized', %s, COALESCE(%s, now())) RETURNING payment_id",
            (merchant[0], customer[0], amount, currency, self.rng.choice(CARD_BRANDS), created_at),
        )
        pid = cur.fetchone()[0]
        if bad_kind:
            self.log_bad("payments", pid, bad_kind)

    def capture(self, cur) -> None:
        row = self.pick_recent(cur, "payments", "payment_id", "status = 'authorized'", window=2000)
        if row:
            cur.execute("UPDATE payments SET status = 'captured' WHERE payment_id = %s", (row[0],))

    def fail_or_void(self, cur) -> None:
        row = self.pick_recent(cur, "payments", "payment_id", "status = 'authorized'", window=2000)
        if not row:
            return
        if self.rng.random() < 0.7:
            cur.execute("UPDATE payments SET status = 'failed', failure_reason = %s WHERE payment_id = %s",
                        (self.rng.choice(FAILURE_REASONS), row[0]))
        else:
            cur.execute("UPDATE payments SET status = 'voided' WHERE payment_id = %s", (row[0],))

    def new_refund(self, cur) -> None:
        # Columns: payment_id, merchant_id, customer_id, amount_cents, ...
        if self.is_bad():
            # Business-rule violations the DB happily accepts.
            kind = self.rng.choice(["refund_exceeds_payment", "refund_on_failed_payment"])
            where = "status = 'captured'" if kind == "refund_exceeds_payment" else "status = 'failed'"
            row = self.pick_recent(cur, "payments", "payment_id", where)
            if not row:
                return
            amt = abs(row[3]) * 2 + 100 if kind == "refund_exceeds_payment" else max(abs(row[3]), 100)
            cur.execute("INSERT INTO refunds (payment_id, amount_cents, reason) VALUES (%s, %s, %s) RETURNING refund_id",
                        (row[0], amt, self.rng.choice(REFUND_REASONS)))
            self.log_bad("refunds", cur.fetchone()[0], kind)
            return

        row = self.pick_recent(cur, "payments", "payment_id", "status = 'captured' AND amount_cents > 0")
        if not row:
            return
        full = self.rng.random() < 0.7
        amt = row[3] if full else max(1, int(row[3] * self.rng.uniform(0.1, 0.9)))
        cur.execute("INSERT INTO refunds (payment_id, amount_cents, reason) VALUES (%s, %s, %s)",
                    (row[0], amt, self.rng.choice(REFUND_REASONS)))
        cur.execute("UPDATE payments SET status = %s WHERE payment_id = %s",
                    ("refunded" if full else "partially_refunded", row[0]))

    def settle_refund(self, cur) -> None:
        row = self.pick_recent(cur, "refunds", "refund_id", "status = 'pending'", window=2000)
        if row:
            new_status = "succeeded" if self.rng.random() < 0.95 else "failed"
            cur.execute("UPDATE refunds SET status = %s WHERE refund_id = %s", (new_status, row[0]))

    def open_dispute(self, cur) -> None:
        row = self.pick_recent(cur, "payments", "payment_id", "status = 'captured' AND amount_cents > 0")
        if row:
            cur.execute("INSERT INTO disputes (payment_id, amount_cents, reason) VALUES (%s, %s, %s)",
                        (row[0], row[3], self.rng.choice(DISPUTE_REASONS)))

    def resolve_dispute(self, cur) -> None:
        row = self.pick_recent(cur, "disputes", "dispute_id", "status = 'open'", window=10**9)
        if row:
            cur.execute("UPDATE disputes SET status = %s WHERE dispute_id = %s",
                        ("won" if self.rng.random() < 0.4 else "lost", row[0]))

    def new_customer(self, cur) -> None:
        cur.execute("INSERT INTO customers (email, country) VALUES (%s, %s)",
                    (self.fake.unique.email(), self.rng.choice(COUNTRIES)))

    def update_merchant(self, cur) -> None:
        # Risk tier and fee changes are what the SCD2 merchant dimension tracks.
        row = self.pick_recent(cur, "merchants", "merchant_id", "TRUE", window=10**9)
        if not row:
            return
        if self.rng.random() < 0.6:
            cur.execute("UPDATE merchants SET risk_tier = %s WHERE merchant_id = %s",
                        (self.rng.choice(RISK_TIERS), row[0]))
        else:
            cur.execute("UPDATE merchants SET fee_bps = %s WHERE merchant_id = %s",
                        (self.rng.choice([250, 290, 320, 350]), row[0]))

    def new_merchant(self, cur) -> None:
        country = self.rng.choice(COUNTRIES)
        bad = self.is_bad()
        cur.execute(
            "INSERT INTO merchants (name, category, country) VALUES (%s, %s, %s) RETURNING merchant_id",
            (self.fake.company(), self.rng.choice(CATEGORIES), None if bad else country),
        )
        mid = cur.fetchone()[0]
        if bad:
            self.log_bad("merchants", mid, "null_country")

    def delete_customer(self, cur) -> None:
        # Hard delete. FK is ON DELETE SET NULL, so Postgres also UPDATEs that
        # customer's payments; CDC captures those cascaded updates too.
        row = self.pick_recent(cur, "customers", "customer_id", "TRUE", window=10**9)
        if row:
            cur.execute("DELETE FROM customers WHERE customer_id = %s", (row[0],))

    # --------------------------------------------------------------------- loop
    def step(self) -> None:
        name = self.rng.choices(list(ACTIONS), weights=list(ACTIONS.values()))[0]
        try:
            with self.conn.cursor() as cur:
                getattr(self, name)(cur)
            self.conn.commit()
            self.stats[name] += 1
        except psycopg.Error as e:
            # Don't crash the whole simulator over one failed action (e.g. a
            # race on a row another action just changed). Roll back and move on.
            self.conn.rollback()
            self.stats["errors"] += 1
            if self.stats["errors"] <= 5:
                print(f"[warn] {name} failed: {e}")

    def close(self) -> None:
        self.bad_log.close()


def main() -> None:
    p = argparse.ArgumentParser(description="PayFlow payments traffic simulator")
    p.add_argument("--rate", type=float, default=20, help="actions per second (default 20)")
    p.add_argument("--duration", type=int, default=0, help="seconds to run, 0 = until Ctrl+C")
    p.add_argument("--bad-rate", type=float, default=0.02, help="probability an eligible action injects bad data")
    p.add_argument("--merchants", type=int, default=200)
    p.add_argument("--customers", type=int, default=5000)
    p.add_argument("--seed", type=int, default=None, help="set for reproducible runs")
    args = p.parse_args()

    stop = False

    def handle_sigint(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    with psycopg.connect(PG_DSN) as conn:
        sim = Simulator(conn, bad_rate=args.bad_rate, seed=args.seed)
        sim.bootstrap(args.merchants, args.customers)

        interval = 1.0 / args.rate
        started = last_report = time.monotonic()
        print(f"Simulating at ~{args.rate}/s, bad-rate={args.bad_rate}. Ctrl+C to stop.")

        while not stop:
            t0 = time.monotonic()
            sim.step()
            if args.duration and t0 - started >= args.duration:
                break
            if t0 - last_report >= 10:
                total = sum(v for k, v in sim.stats.items() if not k.startswith("bad:"))
                print(f"[{int(t0 - started)}s] actions={total} "
                      f"bad={sum(v for k, v in sim.stats.items() if k.startswith('bad:'))} "
                      f"errors={sim.stats['errors']}")
                last_report = t0
            # Simple pacing. Tradeoff: not exact under load, but good enough.
            time.sleep(max(0.0, interval - (time.monotonic() - t0)))

        sim.close()
        print("\nFinal counts:")
        for k, v in sorted(sim.stats.items()):
            print(f"  {k:24s} {v}")


if __name__ == "__main__":
    main()
```

#### `consumer/consumer.py`

```python
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


def parse_event(msg: Message) -> dict | None:
    """
    Turn one Debezium Kafka message into a flat bronze row.
    Returns None for messages that carry no row change (e.g. heartbeats).
    Raises ValueError for malformed messages (they go to the dead-letter file).
    """
    if msg.value() is None:
        return None  # tombstone; disabled in our connector, but be defensive

    value = json.loads(msg.value())
    if "op" not in value:
        return None  # not a row-change event

    source = value.get("source", {})
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
        "_ingested_at": datetime.now(timezone.utc),
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

    def flush(self) -> int:
        """Write every non-empty buffer to its own Parquet file. Returns rows written."""
        written = 0
        now = datetime.now(timezone.utc)
        for table, rows in self.buffers.items():
            if not rows:
                continue
            # Partition folders by table and ingest date:
            #   landing/payments/ingest_date=2026-10-04/part-...parquet
            # WHY: lets Auto Loader / Airflow process one table or one day at a
            # time, and makes backfills of a single day easy.
            out_dir = self.root / table / f"ingest_date={now:%Y-%m-%d}"
            out_dir.mkdir(parents=True, exist_ok=True)

            # Filename includes min/max offsets for traceability: you can tell
            # exactly which Kafka range a file contains. uuid avoids collisions.
            offsets = [r["_kafka_offset"] for r in rows]
            name = f"part-{now:%H%M%S}-{min(offsets)}-{max(offsets)}-{uuid.uuid4().hex[:8]}.parquet"
            final_path = out_dir / name
            tmp_path = out_dir / (name + ".tmp")

            table_arrow = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
            # zstd: better compression than snappy at similar speed. Smaller
            # files = faster uploads to Databricks.
            pq.write_table(table_arrow, tmp_path, compression="zstd")
            os.replace(tmp_path, final_path)  # atomic rename
            written += len(rows)
        self.buffers.clear()
        return written


def main() -> None:
    p = argparse.ArgumentParser(description="Kafka -> Parquet landing consumer")
    # BATCH SIZE / FLUSH INTERVAL TRADEOFF:
    #   bigger batches -> fewer, larger files (Databricks and Parquet love this)
    #   smaller/faster flushes -> lower latency, but many tiny files ("small
    #   file problem": slow listing, slow queries). Flush on whichever comes
    #   first so quiet periods still produce data within --flush-seconds.
    p.add_argument("--batch-size", type=int, default=5000, help="flush after this many events")
    p.add_argument("--flush-seconds", type=float, default=30, help="flush at least this often")
    p.add_argument("--group-id", default="payflow-landing")
    args = p.parse_args()

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
    consumer.subscribe([TOPIC_PATTERN])

    writer = LandingWriter(LANDING_DIR)
    # Highest offset seen per partition since the last commit.
    pending_offsets: dict[tuple[str, int], int] = {}
    last_flush = time.monotonic()
    total = 0
    running = True

    def stop(*_):
        nonlocal running
        running = False

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
            print("CHAOS: crashing after write, before commit")
            os._exit(137)
        # Commit offset+1: Kafka's committed offset means "next message to read".
        consumer.commit(
            offsets=[TopicPartition(t, p, o + 1) for (t, p), o in pending_offsets.items()],
            asynchronous=False,   # block until Kafka confirms; correctness > speed
        )
        pending_offsets.clear()
        total += n
        last_flush = time.monotonic()
        print(f"[{datetime.now():%H:%M:%S}] flushed {n} events (total {total})")

    print(f"Consuming {TOPIC_PATTERN} from {BOOTSTRAP} -> {LANDING_DIR}")
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    # Partition EOF is informational, not an error.
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(msg.error())
                else:
                    try:
                        row = parse_event(msg)
                        if row:
                            writer.add(row)
                    except (ValueError, KeyError, TypeError) as e:
                        writer.dead_letter(msg, repr(e))
                    # Track the offset even for skipped/dead-lettered messages
                    # so we don't re-read them forever.
                    pending_offsets[(msg.topic(), msg.partition())] = msg.offset()

            if writer.size() >= args.batch_size or time.monotonic() - last_flush >= args.flush_seconds:
                flush_and_commit()
    finally:
        # Graceful shutdown: flush what we have so Ctrl+C never loses buffered
        # events, then leave the consumer group cleanly (faster rebalance).
        try:
            flush_and_commit()
        finally:
            consumer.close()
        print(f"Stopped. Total events landed: {total}")


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        sys.exit(f"Kafka error: {e}")
```

#### `scripts/check_landing.py`

```python
"""
Sanity-check the landing zone and print Phase 1 metrics.

Run:  python scripts/check_landing.py

Answers three questions:
  1. Did every table's events arrive, and what kinds (insert/update/delete)?
  2. Any duplicate events? (expected: 0 in normal runs; >0 only after a crash,
     which is fine because silver dedupes on the Kafka offset)
  3. How fast? Latency = time we wrote the file minus time Postgres committed
     the change. This is your first resume number.

WHY DUCKDB: it queries a folder of Parquet files with plain SQL, in-process, no
server, free. Same SQL you'd write in Databricks later.
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "landing"
ARCHIVE = ROOT / "landing_archive"   # files the uploader already sent to Databricks


def main() -> None:
    dirs = [d for d in (LANDING, ARCHIVE) if any(d.glob("*/*/*.parquet"))]
    if not dirs:
        raise SystemExit(f"No Parquet files under {LANDING}. Is the consumer running?")
    FILES = "[" + ",".join(f"'{d}/*/*/*.parquet'" for d in dirs) + "]"

    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet({FILES})")

    print("\n== Events by table and operation (c=insert, u=update, d=delete, r=snapshot) ==")
    print(con.sql("""
        SELECT table_name, op, COUNT(*) AS events
        FROM events GROUP BY 1, 2 ORDER BY 1, 2
    """))

    print("\n== Duplicate check (same Kafka topic/partition/offset landed twice) ==")
    print(con.sql("""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT (_kafka_topic, _kafka_partition, _kafka_offset)) AS unique_events,
               COUNT(*) - COUNT(DISTINCT (_kafka_topic, _kafka_partition, _kafka_offset)) AS duplicates
        FROM events
    """))

    # Snapshot reads (op='r') are excluded: their source_ts_ms is the snapshot
    # time, not a real change, so they'd distort latency.
    # Note: this latency includes the consumer's flush wait. With
    # --flush-seconds 30, expect p50 around 15s. Lower flush = lower latency =
    # more small files. That's the tradeoff to talk about.
    print("\n== End-to-end latency: Postgres commit -> Parquet file on disk (seconds) ==")
    print(con.sql("""
        WITH l AS (
            SELECT (epoch_ms(_ingested_at) - source_ts_ms) / 1000.0 AS sec
            FROM events WHERE op <> 'r' AND source_ts_ms IS NOT NULL
        )
        SELECT COUNT(*) AS events,
               ROUND(quantile_cont(sec, 0.50), 2) AS p50_s,
               ROUND(quantile_cont(sec, 0.95), 2) AS p95_s,
               ROUND(quantile_cont(sec, 0.99), 2) AS p99_s,
               ROUND(MAX(sec), 2)                 AS max_s
        FROM l
    """))

    print("\n== Files written (watch for too many tiny files) ==")
    print(con.sql(f"""
        SELECT COUNT(DISTINCT filename) AS files,
               ROUND(COUNT(*) / COUNT(DISTINCT filename), 0) AS avg_events_per_file
        FROM read_parquet({FILES}, filename = true)
    """))

    dlq = LANDING / "_dead_letter" / "events.jsonl"
    print(f"\nDead-letter events: {sum(1 for _ in dlq.open()) if dlq.exists() else 0}")


if __name__ == "__main__":
    main()
```

#### `scripts/verify_no_loss.py`

```python
"""
Prove the CDC path lost nothing: compare Postgres (truth) with the landing
files (what the pipeline captured), row by row.

Run AFTER stopping the simulator and letting the consumer drain:
    python scripts/verify_no_loss.py

For every table:
  - every primary key in Postgres must have a latest landed event that is not a delete
  - every key whose latest landed event is a delete must be gone from Postgres
  - for payments: the latest landed status must equal the Postgres status
    (proves UPDATES arrived in order, not just inserts)

Exit code 1 on any difference. Used by CI and by every chaos scenario.

WHY CHECK LANDING (not silver): this isolates the capture half of the system
(Postgres -> Debezium -> Kafka -> consumer). Silver is checked separately by
reconcile.py --mode full. When something breaks, you know which half.
"""

import os
import sys
from pathlib import Path

import duckdb
import psycopg

ROOT = Path(__file__).resolve().parent.parent
LANDING = Path(os.getenv("LANDING_DIR", ROOT / "landing"))
ARCHIVE = Path(os.getenv("ARCHIVE_DIR", ROOT / "landing_archive"))
PG_DSN = os.getenv("PG_DSN", "postgresql://payflow:payflow@localhost:5432/payflow")
PKS = {"merchants": "merchant_id", "customers": "customer_id", "payments": "payment_id",
       "refunds": "refund_id", "disputes": "dispute_id"}


def latest_events(con, table: str, pk: str):
    """Latest landed event per key -> {pk: (op, status)}."""
    globs = [str(d / table / "*" / "*.parquet") for d in (LANDING, ARCHIVE) if any((d / table).glob("*/*.parquet"))]
    if not globs:
        return {}
    files = "[" + ",".join(f"'{g}'" for g in globs) + "]"
    rows = con.sql(f"""
        SELECT CAST(json_extract(primary_key, '$.{pk}') AS BIGINT) AS id, op,
               json_extract_string(after, '$.status') AS status
        FROM read_parquet({files})
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY source_lsn DESC, _kafka_offset DESC) = 1
    """).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def diff_table(pg_rows: dict, landed: dict, check_status: bool) -> dict:
    """Pure comparison (unit tested). pg_rows: {pk: status}, landed: {pk: (op, status)}."""
    alive = {k for k, (op, _) in landed.items() if op != "d"}
    missing = sorted(set(pg_rows) - alive)                  # in Postgres, not captured
    extra = sorted(alive - set(pg_rows))                    # captured as alive, but gone in Postgres (missed delete)
    wrong = sorted(k for k in set(pg_rows) & alive
                   if check_status and pg_rows[k] != landed[k][1])
    return {"pg": len(pg_rows), "landed_alive": len(alive), "missing": missing[:10], "n_missing": len(missing),
            "extra": extra[:10], "n_extra": len(extra), "wrong_status": wrong[:10], "n_wrong": len(wrong)}


def main() -> None:
    con = duckdb.connect()
    failed = False
    with psycopg.connect(PG_DSN) as pg, pg.cursor() as cur:
        for table, pk in PKS.items():
            has_status = table in ("payments", "refunds", "disputes")
            cur.execute(f"SELECT {pk}, {'status' if has_status else 'NULL'} FROM {table}")
            pg_rows = {r[0]: r[1] for r in cur.fetchall()}
            d = diff_table(pg_rows, latest_events(con, table, pk), check_status=has_status)
            ok = d["n_missing"] == d["n_extra"] == d["n_wrong"] == 0
            failed |= not ok
            print(f"{'OK  ' if ok else 'FAIL'} {table:10s} postgres={d['pg']:>7} landed={d['landed_alive']:>7} "
                  f"missing={d['n_missing']} extra={d['n_extra']} wrong_status={d['n_wrong']}")
            if not ok:
                print(f"      samples: missing={d['missing']} extra={d['extra']} wrong={d['wrong_status']}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
```

### Phase 2: Upload and deploy

#### `payflow_common/connections.py`

```python
"""
Connection helpers shared by the uploader, reconciliation, and Airflow DAGs.
All settings come from environment variables (see .env.example), never code.

WHY ENV VARS: the same code runs on your laptop, inside the Airflow container,
and in CI. Only the environment changes. Secrets stay out of git.
"""

import os

import psycopg


def catalog() -> str:
    return os.getenv("PAYFLOW_CATALOG", "payflow")


def pg_connect() -> psycopg.Connection:
    """Source Postgres (read-only usage in reconciliation)."""
    return psycopg.connect(os.getenv("PG_DSN", "postgresql://payflow:payflow@localhost:5432/payflow"))


def dbx_sql_connect():
    """
    Databricks SQL warehouse connection (the one 2X-Small warehouse in Free Edition).
    Imported lazily so unit tests don't need the package.
    """
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    return sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        # Same time zone as Postgres-side queries, so DATE() buckets match.
        session_configuration={"timezone": "UTC"},
    )


def dbx_query(sql_text: str) -> list[tuple]:
    with dbx_sql_connect() as conn, conn.cursor() as cur:
        cur.execute(sql_text)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []


def dbx_query_dicts(sql_text: str) -> list[dict]:
    """Same as dbx_query, but rows as {column_name: value}. Use when column order isn't guaranteed."""
    with dbx_sql_connect() as conn, conn.cursor() as cur:
        cur.execute(sql_text)
        if not cur.description:
            return []
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
```

#### `uploader/upload_to_volume.py`

```python
"""
Upload landed Parquet files to a Databricks Unity Catalog Volume, then move
them to a local archive.

Run:  python uploader/upload_to_volume.py      (Airflow calls upload_all())

Flow per file:
    landing/payments/ingest_date=.../part-x.parquet
      -> /Volumes/<catalog>/raw/files/landing/payments/ingest_date=.../part-x.parquet
      -> landing_archive/payments/ingest_date=.../part-x.parquet   (local)

WHY "UPLOAD THEN MOVE" (no manifest database):
    The landing folder itself is the to-do list: whatever is still in it hasn't
    been uploaded. A crash after upload but before the move just means the file
    is uploaded again to the SAME path with overwrite=True. Auto Loader doesn't
    re-ingest a path it already processed, and even if it did, silver dedupes
    on the Kafka event id. So the worst case is wasted bandwidth, never
    duplicated data.

WHY KEEP AN ARCHIVE (instead of deleting):
    Cheap insurance. If the Volume is wiped, we can re-upload everything.
    Production would set a retention policy (e.g. 30 days) on it.

WHY ONLY *.parquet (not .tmp):
    The consumer writes .tmp then renames. Ignoring .tmp means we never upload
    a half-written file.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from payflow_common.connections import catalog  # noqa: E402

LANDING = Path(os.getenv("LANDING_DIR", ROOT / "landing"))
ARCHIVE = Path(os.getenv("ARCHIVE_DIR", ROOT / "landing_archive"))
GROUND_TRUTH = Path(os.getenv("GROUND_TRUTH", ROOT / "simulator" / "injected" / "bad_records.jsonl"))


def volume_root() -> str:
    return f"/Volumes/{catalog()}/raw/files"


def pending_files(landing: Path = LANDING) -> list[Path]:
    """Completed Parquet files waiting to be uploaded, oldest first."""
    return sorted(landing.glob("*/ingest_date=*/*.parquet"), key=lambda p: p.stat().st_mtime)


def upload_all(client=None, landing: Path = LANDING, archive: Path = ARCHIVE) -> int:
    """Upload every pending file. Returns how many files were uploaded."""
    if client is None:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()  # DATABRICKS_HOST / DATABRICKS_TOKEN

    files = pending_files(landing)
    t0, total_bytes = time.monotonic(), 0
    for p in files:
        rel = p.relative_to(landing).as_posix()
        with p.open("rb") as f:
            client.files.upload(f"{volume_root()}/landing/{rel}", f, overwrite=True)
        total_bytes += p.stat().st_size
        dest = archive / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(p, dest)  # only after a successful upload

    # Ground truth for DQ catch-rate. Whole file, overwritten each time (small).
    if GROUND_TRUTH.exists():
        with GROUND_TRUTH.open("rb") as f:
            client.files.upload(f"{volume_root()}/ground_truth/bad_records.jsonl", f, overwrite=True)

    print(f"uploaded {len(files)} files, {total_bytes / 1e6:.2f} MB in {time.monotonic() - t0:.1f}s")
    return len(files)


if __name__ == "__main__":
    upload_all()
```

#### `databricks/deploy.py`

```python
"""
Deploy notebooks + shared module to the Databricks workspace and create/update
the lakehouse job. Safe to re-run (idempotent).

Run:  python databricks/deploy.py
Needs: DATABRICKS_HOST, DATABRICKS_TOKEN (or a `databricks auth login` profile)

WHY DEPLOY FROM CODE instead of clicking in the UI:
    The job definition (task order, parameters, concurrency) is versioned in
    git next to the notebooks. A fresh workspace is one command away, and the
    README never says "click here, then here".

WHY SERVERLESS (no cluster spec on tasks):
    Free Edition is serverless-only. Leaving out cluster config makes Databricks
    run each task on serverless compute. No idle clusters, no cluster tuning.
"""

import io
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace

JOB_NAME = "payflow-lakehouse"
CATALOG = os.getenv("PAYFLOW_CATALOG", "payflow")
NB_DIR = Path(__file__).resolve().parent / "notebooks"

# task_key -> (notebook file stem, upstream task)
TASKS = [
    ("setup", "00_setup", None),
    ("bronze", "01_bronze", "setup"),
    ("silver", "02_silver", "bronze"),
    ("quality", "03_quality", "silver"),
    ("gold", "04_gold", "quality"),
]


def main() -> None:
    w = WorkspaceClient()  # reads DATABRICKS_HOST / DATABRICKS_TOKEN from env
    me = w.current_user.me().user_name
    base = f"/Workspace/Users/{me}/payflow"
    w.workspace.mkdirs(base)

    # ImportFormat.AUTO: files starting with "# Databricks notebook source"
    # become notebooks (extension dropped); transforms.py becomes a plain
    # workspace file that the notebooks can `import`.
    for f in sorted(NB_DIR.glob("*.py")):
        w.workspace.upload(f"{base}/{f.name}", io.BytesIO(f.read_bytes()),
                           format=workspace.ImportFormat.AUTO, overwrite=True)
        print(f"uploaded {f.name}")

    settings = jobs.JobSettings(
        name=JOB_NAME,
        # One run at a time. Silver's high-watermark relies on it, and it keeps
        # Free Edition's 5-concurrent-task limit far away.
        max_concurrent_runs=1,
        # Job parameters are pushed to every notebook's widgets.
        parameters=[
            jobs.JobParameterDefinition(name="catalog", default=CATALOG),
            jobs.JobParameterDefinition(name="full_refresh", default="false"),
        ],
        tasks=[
            jobs.Task(
                task_key=key,
                notebook_task=jobs.NotebookTask(notebook_path=f"{base}/{stem}"),
                depends_on=[jobs.TaskDependency(task_key=up)] if up else None,
                max_retries=1,
                timeout_seconds=1800,
            )
            for key, stem, up in TASKS
        ],
    )

    existing = list(w.jobs.list(name=JOB_NAME))
    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings=settings)
        print(f"updated job {JOB_NAME} ({job_id})")
    else:
        job_id = w.jobs.create(**settings.as_shallow_dict()).job_id
        print(f"created job {JOB_NAME} ({job_id})")


if __name__ == "__main__":
    main()
```

### Phase 3-4: Databricks lakehouse

#### `databricks/notebooks/transforms.py`

```python
"""
Shared transformation logic for the PayFlow lakehouse.

Imported by the Databricks notebooks (deployed as a workspace file in the same
folder, so `import transforms` works) AND by local unit tests with plain PySpark.

WHY A SHARED MODULE INSTEAD OF LOGIC INSIDE NOTEBOOKS:
    Notebooks are hard to unit test and easy to copy-paste between. Pure
    functions that take and return DataFrames / SQL strings can be tested on a
    laptop with local Spark, and the notebooks become thin wrappers that only do
    I/O (read table, call function, write table).

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
    """
    h = fq("silver", "merchants_history")
    attrs = ("concat_ws('|', coalesce(name,''), coalesce(category,''), coalesce(country,''), "
             "coalesce(risk_tier,''), coalesce(cast(fee_bps AS STRING),''), coalesce(status,''))")
    return f"""
    WITH ordered AS (
        SELECT merchant_id, name, category, country, risk_tier, fee_bps, status, op,
               _source_ts, source_lsn, _kafka_offset,
               {attrs} AS attrs,
               LAG({attrs}) OVER (PARTITION BY merchant_id ORDER BY source_lsn, _kafka_offset) AS prev_attrs
        FROM {h}
    ),
    versions AS (
        SELECT * FROM ordered
        WHERE op <> 'd' AND (prev_attrs IS NULL OR prev_attrs <> attrs)
    )
    SELECT merchant_id, name, category, country, risk_tier, fee_bps, status,
           _source_ts AS valid_from,
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
```

#### `databricks/notebooks/00_setup.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: schemas, volumes, ops tables
# MAGIC Idempotent (`IF NOT EXISTS` everywhere), so it runs as the first task of every job run.
# MAGIC Cost is a few metadata calls. In exchange, a fresh workspace never fails on a missing table.

# COMMAND ----------

dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# Medallion layers as schemas. WHY SCHEMAS, not table-name prefixes: permissions
# can be granted per layer (analysts get gold only), and names stay short.
for schema in ["raw", "bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Volumes: governed file storage inside Unity Catalog. Free Edition has limited
# DBFS access, so files and checkpoints live in volumes.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.files")         # landing + ground truth
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")   # Auto Loader state

# COMMAND ----------

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
```

#### `databricks/notebooks/01_bronze.py`

```python
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

# COMMAND ----------

dbutils.widgets.text("catalog", "payflow")
dbutils.widgets.text("full_refresh", "false")
catalog = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{catalog}/raw/files/landing/"
CHECKPOINT = f"/Volumes/{catalog}/ops/checkpoints/bronze_cdc_events"
TARGET = f"{catalog}.bronze.cdc_events"

# COMMAND ----------

from pyspark.sql import functions as F

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    # Where Auto Loader stores the inferred schema between runs.
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/_schema")
    # Only Parquet. Dead-letter and ground-truth JSONL files are not CDC events.
    .option("pathGlobFilter", "*.parquet")
    .load(LANDING)
    # Lineage: which file each row came from. Answers "where did this row come from?"
    .withColumn("_source_file", F.col("_metadata.file_path"))
    # Load time, used as silver's incremental high-watermark.
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
```

#### `databricks/notebooks/02_silver.py`

```python
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

# COMMAND ----------

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

# Full refresh = rebuild silver from bronze. Used for replays after a logic fix
# and for the determinism test. Bronze is untouched (it's the source of truth).
if full_refresh:
    for table in T.TABLE_SPECS:
        spark.sql(f"DROP VIEW IF EXISTS {fq('silver', table)}")
        spark.sql(f"DROP TABLE IF EXISTS {fq('silver', table + '_history')}")
        spark.sql(f"DROP TABLE IF EXISTS {fq('silver', table + '_state')}")
    spark.sql(f"DELETE FROM {fq('ops', 'watermarks')} WHERE pipeline = 'silver'")
    print("Full refresh: silver dropped, watermark reset")

# COMMAND ----------

wm_row = spark.sql(f"SELECT MAX(value) AS v FROM {fq('ops', 'watermarks')} WHERE pipeline = 'silver'").first()
watermark = wm_row["v"] if wm_row else None

new_events = spark.table(BRONZE)
if watermark:
    new_events = new_events.filter(F.col("_bronze_loaded_at") > F.lit(watermark).cast("timestamp"))

# Materialize the batch once. Serverless doesn't allow df.cache(), and without
# this, every table below would re-scan bronze (5 tables x several actions).
T.dedupe_events(new_events).write.mode("overwrite").saveAsTable(STAGE)
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


if events_processed > 0:
    for table, spec in T.TABLE_SPECS.items():
        pk = spec["pk"]
        changes = T.parse_changes(batch, table)
        if changes.isEmpty():
            continue
        hist, state = fq("silver", f"{table}_history"), fq("silver", f"{table}_state")
        ensure_table(hist, changes)
        ensure_table(state, changes)

        # 1) History: insert each event exactly once, keyed by Kafka event id.
        #    Insert-only MERGE (instead of append) makes replays and retries safe.
        (DeltaTable.forName(spark, hist).alias("t")
            .merge(changes.alias("s"),
                   "t._kafka_topic = s._kafka_topic AND t._kafka_partition = s._kafka_partition "
                   "AND t._kafka_offset = s._kafka_offset")
            .withSchemaEvolution()        # new contract columns are added automatically
            .whenNotMatchedInsertAll()
            .execute())

        # 2) State: apply only the newest event per key, and only if it is newer
        #    than what we have (LSN order). Deletes are written as tombstones
        #    instead of removing the row.
        #    WHY TOMBSTONES: with at-least-once delivery, an old duplicate
        #    "insert" can arrive AFTER the delete was applied. With a hard delete
        #    there's no row to compare against, so the old insert would bring the
        #    deleted customer back. The tombstone's LSN blocks it.
        latest = T.latest_per_key(changes, pk)
        (DeltaTable.forName(spark, state).alias("t")
            .merge(latest.alias("s"), f"t.{pk} = s.{pk}")
            .withSchemaEvolution()
            .whenMatchedUpdateAll(condition=T.NEWER)
            .whenNotMatchedInsertAll()
            .execute())

        # 3) Schema drift: record unknown columns (insert-only).
        drift = T.schema_drift(batch, table)
        if not drift.isEmpty():
            (DeltaTable.forName(spark, fq("ops", "schema_drift_events")).alias("t")
                .merge(drift.alias("s"), "t.table_name = s.table_name AND t.column_name = s.column_name")
                .whenNotMatchedInsertAll()
                .execute())
            print(f"SCHEMA DRIFT in {table}: {[r['column_name'] for r in drift.collect()]}")

        spark.sql(f"CREATE OR REPLACE VIEW {fq('silver', table)} AS SELECT * FROM {state} WHERE NOT _is_deleted")
        print(f"{table}: {changes.count()} events applied")

    # Advance the watermark only after every table succeeded. If anything above
    # failed, the next run reprocesses the same events, which is safe (idempotent).
    spark.sql(f"""
        MERGE INTO {fq('ops', 'watermarks')} t
        USING (SELECT 'silver' AS pipeline, '{high}' AS value) s ON t.pipeline = s.pipeline
        WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (pipeline, value, updated_at) VALUES (s.pipeline, s.value, current_timestamp())
    """)
    print(f"watermark advanced to {high}")
```

#### `databricks/notebooks/03_quality.py`

```python
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

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import transforms as T


def fq(schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


if not spark.catalog.tableExists(fq("silver", "payments_state")):
    dbutils.notebook.exit("silver not built yet")

# COMMAND ----------

# Insert-only: a violation is recorded once with the time we first saw it.
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

# Ground truth uploaded by the uploader. Absent until the simulator injects something.
GT = f"/Volumes/{catalog}/raw/files/ground_truth/bad_records.jsonl"
try:
    gt = spark.read.json(GT).withColumnRenamed("table", "table_name").withColumn("pk", F.col("pk").cast("string"))
    gt.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("ops", "injected_bad_records"))
except Exception as e:  # file not uploaded yet
    print(f"No ground truth yet: {e}")

if spark.catalog.tableExists(fq("ops", "injected_bad_records")):
    rate = spark.sql(T.dq_catch_rate_sql(fq)).withColumn("run_ts", F.current_timestamp())
    rate.select("run_ts", "injected", "caught", "false_positives", "flagged_total") \
        .write.mode("append").saveAsTable(fq("ops", "dq_catch_rate_history"))
    display(rate)
```

#### `databricks/notebooks/04_gold.py`

```python
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

# Order matters: settlement and risk read the dimension and the lifecycle fact.
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

# Pipeline health row. Latency = Postgres commit -> row visible in silver, for
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
```

### Phase 4: Reconciliation

#### `reconciliation/reconcile.py`

```python
"""
Reconcile the source Postgres against the lakehouse silver layer, to the cent.

Run:
    python reconciliation/reconcile.py --mode daily --days 3
    python reconciliation/reconcile.py --mode full     # only after stopping the simulator

TWO MODES, because CDC is always slightly behind the source:

  daily  Runs while traffic flows (Airflow, every night). Compares only facts
         that never change after insert (row count and SUM(amount_cents) per
         created day) for CLOSED days (before today, UTC). Rows inserted today
         are excluded, so pipeline lag can't cause false alarms. Expect an exact
         match, every day.

  full   Quiesced check (simulator stopped, pipeline drained). Compares
         everything including mutable state: rows per status per table, and
         customer count (proves deletes propagated). Used after chaos tests.

WHY NOT A ROW-BY-ROW HASH COMPARE: Postgres and Spark hash differently, and
pulling every row out of both systems doesn't scale. Grouped counts and sums
in integer cents catch missing rows, duplicates and wrong amounts at a tiny
cost. The tradeoff: two errors that cancel out exactly (one row missing, one
duplicated with the same amount) would slip through. Status-level counts in
full mode narrow that further.

Exit code 1 on any mismatch, so Airflow marks the task failed and alerts.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from payflow_common.connections import catalog, dbx_query, pg_connect  # noqa: E402

AMOUNT_TABLES = ["payments", "refunds", "disputes"]


def daily_queries(start: date, end: date) -> tuple[dict[str, str], dict[str, str]]:
    """(postgres_sql, databricks_sql) per table: day, count, sum of cents."""
    pg, dbx = {}, {}
    for t in AMOUNT_TABLES:
        pg[t] = (f"SELECT (created_at AT TIME ZONE 'UTC')::date::text, COUNT(*), COALESCE(SUM(amount_cents),0) "
                 f"FROM {t} WHERE created_at >= '{start}' AND created_at < '{end}' GROUP BY 1")
        dbx[t] = (f"SELECT CAST(TO_DATE(created_at) AS STRING), COUNT(*), COALESCE(SUM(amount_cents),0) "
                  f"FROM {catalog()}.silver.{t} WHERE created_at >= '{start}' AND created_at < '{end}' GROUP BY 1")
    return pg, dbx


def full_queries() -> tuple[dict[str, str], dict[str, str]]:
    """Status-level counts/sums for every table, plus customers (deletes)."""
    pg, dbx = {}, {}
    for t in AMOUNT_TABLES:
        pg[t] = f"SELECT status, COUNT(*), COALESCE(SUM(amount_cents),0) FROM {t} GROUP BY 1"
        dbx[t] = f"SELECT status, COUNT(*), COALESCE(SUM(amount_cents),0) FROM {catalog()}.silver.{t} GROUP BY 1"
    pg["merchants"] = "SELECT risk_tier, COUNT(*), COALESCE(SUM(fee_bps),0) FROM merchants GROUP BY 1"
    dbx["merchants"] = f"SELECT risk_tier, COUNT(*), COALESCE(SUM(fee_bps),0) FROM {catalog()}.silver.merchants GROUP BY 1"
    pg["customers"] = "SELECT 'all', COUNT(*), 0 FROM customers"
    dbx["customers"] = f"SELECT 'all', COUNT(*), 0 FROM {catalog()}.silver.customers"
    return pg, dbx


def compare(source: dict[str, list[tuple]], lake: dict[str, list[tuple]]) -> list[dict]:
    """
    Pure comparison (unit tested). Rows are (key, count, sum).
    Returns one result per (table, key) present on either side.
    """
    results = []
    for table in sorted(set(source) | set(lake)):
        src = {str(r[0]): (int(r[1]), int(r[2])) for r in source.get(table, [])}
        lak = {str(r[0]): (int(r[1]), int(r[2])) for r in lake.get(table, [])}
        for key in sorted(set(src) | set(lak)):
            sv, lv = src.get(key, (0, 0)), lak.get(key, (0, 0))
            results.append({
                "table": table, "key": key,
                "source_count": sv[0], "lake_count": lv[0],
                "source_cents": sv[1], "lake_cents": lv[1],
                "match": sv == lv,
            })
    return results


def run(mode: str, days: int) -> list[dict]:
    if mode == "daily":
        end = datetime.now(timezone.utc).date()          # today is still open: excluded
        pg_sql, dbx_sql = daily_queries(end - timedelta(days=days), end)
    else:
        pg_sql, dbx_sql = full_queries()

    with pg_connect() as conn, conn.cursor() as cur:
        source = {}
        for t, q in pg_sql.items():
            cur.execute(q)
            source[t] = cur.fetchall()
    lake = {t: dbx_query(q) for t, q in dbx_sql.items()}
    return compare(source, lake)


def record(mode: str, results: list[dict]) -> None:
    """Store the run in ops.reconciliation_runs so Tableau can chart it."""
    mismatches = [r for r in results if not r["match"]]
    details = json.dumps(mismatches[:50]).replace("'", "''")
    dbx_query(
        f"INSERT INTO {catalog()}.ops.reconciliation_runs VALUES "
        f"(current_timestamp(), '{mode}', {len(results)}, {len(mismatches)}, '{details}')"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["daily", "full"], default="daily")
    p.add_argument("--days", type=int, default=3, help="closed days to check (daily mode)")
    args = p.parse_args()

    results = run(args.mode, args.days)
    record(args.mode, results)
    bad = [r for r in results if not r["match"]]
    cents = sum(r["source_cents"] for r in results if r["table"] == "payments")
    print(f"{args.mode}: {len(results)} checks, {len(bad)} mismatches, "
          f"${cents / 100:,.2f} of payments compared")
    for r in bad:
        print(f"  MISMATCH {r}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
```

### Phase 5: Airflow

#### `airflow/dags/payflow_lakehouse.py`

```python
"""
Main pipeline DAG, every 30 minutes:

  check_replication_slot -> upload_landing_files -> [skip if nothing new]
      -> run_databricks_job -> check_freshness

WHY AIRFLOW ORCHESTRATES BUT DOESN'T PROCESS:
    Airflow decides WHEN and IN WHAT ORDER; Databricks does the data work.
    Heavy processing inside Airflow workers is a classic anti-pattern: the
    scheduler gets starved and retries become expensive.

WHY EVERY 30 MIN (not every 1 min):
    Each run spins up serverless compute, and Free Edition has a daily compute
    quota. 30 min keeps freshness under ~35 min while staying inside the quota.
    The tradeoff is explicit and tunable: lower the schedule and measure the
    freshness/quota curve (that's a good README chart).

WHY SHORT-CIRCUIT when nothing was uploaded:
    No new files = nothing to process. Skipping the Databricks run saves quota.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Alert thresholds. Tune from measurements, don't guess.
SLOT_LAG_FAIL_BYTES = 512 * 1024 * 1024   # Debezium falling behind: WAL piling up on the source
FRESHNESS_FAIL_SECONDS = 2 * 60 * 60       # silver older than 2h = something's stuck

default_args = {
    "owner": "payflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="payflow_lakehouse",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,            # don't backfill missed intervals: the data is in Kafka/landing anyway
    max_active_runs=1,        # never two uploads/jobs at once (silver watermark relies on order)
    default_args=default_args,
    tags=["payflow"],
)
def payflow_lakehouse():

    @task
    def check_replication_slot() -> int:
        """
        How much WAL is Postgres holding for Debezium?
        If Debezium stops reading, Postgres keeps every WAL segment for the slot
        and can eventually fill its disk and take the payments DB down. This is
        the #1 operational risk of log-based CDC, so we check it every run.
        """
        from payflow_common.connections import pg_connect
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                FROM pg_replication_slots WHERE slot_name = 'payflow_slot'
            """)
            row = cur.fetchone()
        lag = int(row[0]) if row else 0
        print(f"replication slot lag: {lag / 1e6:.2f} MB")
        if lag > SLOT_LAG_FAIL_BYTES:
            raise RuntimeError(f"Replication slot lag {lag / 1e6:.0f} MB > threshold. Is Debezium running?")
        return lag

    @task.short_circuit
    def upload_landing_files() -> bool:
        from uploader.upload_to_volume import upload_all
        return upload_all() > 0          # False -> downstream tasks are skipped

    run_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_name="payflow-lakehouse",     # resolved to a job id at runtime (no hard-coded ids)
        job_parameters={"catalog": os.getenv("PAYFLOW_CATALOG", "payflow"), "full_refresh": "false"},
        wait_for_termination=True,
    )

    @task
    def check_freshness() -> float:
        """Fail loudly if silver hasn't seen a new source change recently."""
        from payflow_common.connections import catalog, dbx_query
        rows = dbx_query(f"SELECT freshness_seconds, latency_p95_s FROM {catalog()}.ops.pipeline_runs "
                         f"ORDER BY run_ts DESC LIMIT 1")
        freshness, p95 = (rows[0] if rows else (None, None))
        print(f"freshness={freshness}s latency_p95={p95}s")
        if freshness is not None and freshness > FRESHNESS_FAIL_SECONDS:
            raise RuntimeError(f"Silver is {freshness:.0f}s behind the source")
        return freshness or 0.0

    check_replication_slot() >> upload_landing_files() >> run_job >> check_freshness()


payflow_lakehouse()
```

#### `airflow/dags/payflow_ops.py`

```python
"""
Operational DAGs:

  payflow_daily_reconciliation  00:37 UTC daily. Source vs lakehouse, to the cent.
  payflow_maintenance           weekly. OPTIMIZE + VACUUM, records file counts.
  payflow_replay                manual. Rebuild silver/gold from bronze.

Kept in one file because they share helpers and are small.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {"owner": "payflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
CATALOG = os.getenv("PAYFLOW_CATALOG", "payflow")


# -----------------------------------------------------------------------------
# Daily reconciliation
# WHY 00:37 and not 00:00: the day just closed; give the 30-min pipeline one
# run to catch up on the last events of the day. Odd minute = not competing
# with every other job scheduled at :00.
# -----------------------------------------------------------------------------
@dag(dag_id="payflow_daily_reconciliation", schedule="37 0 * * *", start_date=datetime(2026, 10, 1),
     catchup=False, default_args=default_args, tags=["payflow", "quality"])
def payflow_daily_reconciliation():

    @task
    def reconcile_closed_days():
        from reconciliation.reconcile import record, run
        results = run("daily", days=3)
        record("daily", results)
        bad = [r for r in results if not r["match"]]
        print(f"{len(results)} checks, {len(bad)} mismatches")
        if bad:
            # Task failure = alert (email/Slack callback in production).
            raise RuntimeError(f"Reconciliation mismatches: {bad[:5]}")

    reconcile_closed_days()


# -----------------------------------------------------------------------------
# Weekly maintenance
# WHY: frequent small micro-batches create many small files ("small file
# problem"), and every query pays to open them. OPTIMIZE compacts them; VACUUM
# deletes files no longer referenced (after the default 7-day retention, which
# keeps time travel working for a week).
# Databricks may also run predictive optimization on managed tables. We still
# schedule it to make the behavior explicit and to MEASURE it.
# -----------------------------------------------------------------------------
MAINTAINED = ["bronze.cdc_events", "silver.payments_history", "silver.payments_state",
              "silver.refunds_history", "silver.merchants_history"]


@dag(dag_id="payflow_maintenance", schedule="13 3 * * 0", start_date=datetime(2026, 10, 1),
     catchup=False, default_args=default_args, tags=["payflow", "ops"])
def payflow_maintenance():

    @task
    def optimize_and_vacuum():
        from payflow_common.connections import dbx_query, dbx_query_dicts
        for t in MAINTAINED:
            name = f"{CATALOG}.{t}"
            files_before = dbx_query_dicts(f"DESCRIBE DETAIL {name}")[0]["numFiles"]
            dbx_query(f"OPTIMIZE {name}")
            dbx_query(f"VACUUM {name}")
            after = dbx_query_dicts(f"DESCRIBE DETAIL {name}")[0]
            files_after, size = after["numFiles"], after["sizeInBytes"]
            dbx_query(f"INSERT INTO {CATALOG}.ops.maintenance_runs VALUES "
                      f"(current_timestamp(), '{t}', {files_before}, {files_after}, {size})")
            print(f"{t}: {files_before} -> {files_after} files")

    optimize_and_vacuum()


# -----------------------------------------------------------------------------
# Replay
# WHY: bronze is immutable, so any silver/gold bug is fixed by deploying the
# corrected code and replaying. Also used to prove determinism: replaying the
# same bronze must produce identical gold totals.
# -----------------------------------------------------------------------------
@dag(dag_id="payflow_replay", schedule=None, start_date=datetime(2026, 10, 1),
     catchup=False, default_args=default_args, tags=["payflow", "ops"])
def payflow_replay():
    DatabricksRunNowOperator(
        task_id="full_refresh_from_bronze",
        databricks_conn_id="databricks_default",
        job_name="payflow-lakehouse",
        job_parameters={"catalog": CATALOG, "full_refresh": "true"},
        wait_for_termination=True,
    )


payflow_daily_reconciliation()
payflow_maintenance()
payflow_replay()
```

### Phase 8: Benchmark and chaos

#### `scripts/benchmark_throughput.py`

```python
"""
Find the highest change-event rate the CDC path sustains without falling behind.

Run (stack up, connector registered, consumer running in another terminal):
    python scripts/benchmark_throughput.py --rates 50 100 200 400 --seconds 60 --procs 4

For each target rate it runs N simulator processes for --seconds, then reads
the consumer group's lag from Kafka.
  events/sec = how many change events Kafka received per second (end offsets)
  final lag  = events produced but not yet landed when the load stops

"Sustained" = final lag stays small (under ~1 flush batch). When lag grows
with every step, you've passed the ceiling.

HONEST CAVEATS for the README:
  - One simulator process tops out around a few hundred actions/sec (one
    Postgres connection, one transaction per action). Use --procs to make sure
    you're measuring the pipeline, not the load generator.
  - One action creates 1-3 change events (a refund = insert refund + update
    payment). Report EVENTS/sec, which is what the pipeline actually handles.
  - Laptop numbers, single broker. Say so next to the number.
"""

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
GROUP = "payflow-landing"


def group_offsets() -> tuple[int, int]:
    """(sum of log-end offsets, sum of lag) across payflow topics for the consumer group."""
    out = subprocess.run(
        ["docker", "exec", "payflow-kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
         "--bootstrap-server", "localhost:9092", "--describe", "--group", GROUP],
        capture_output=True, text=True, check=True).stdout
    end = lag = 0
    for line in out.splitlines():
        parts = re.split(r"\s+", line.strip())
        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(parts) >= 6 and parts[1].startswith("payflow.public.") and parts[4].isdigit():
            end += int(parts[4])
            lag += int(parts[5]) if parts[5].isdigit() else 0
    return end, lag


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rates", type=int, nargs="+", default=[50, 100, 200, 400], help="total actions/sec")
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--procs", type=int, default=4)
    p.add_argument("--settle", type=int, default=35, help="seconds to wait for the consumer's last flush")
    args = p.parse_args()

    print(f"{'target act/s':>12} {'events/s':>9} {'lag@stop':>9} {'lag@settle':>11}")
    for rate in args.rates:
        start_end, _ = group_offsets()
        t0 = time.monotonic()
        procs = [subprocess.Popen([PY, str(ROOT / "simulator" / "simulator.py"),
                                   "--rate", str(rate / args.procs), "--duration", str(args.seconds),
                                   "--bad-rate", "0"], stdout=subprocess.DEVNULL)
                 for _ in range(args.procs)]
        for pr in procs:
            pr.wait()
        elapsed = time.monotonic() - t0
        end, lag_stop = group_offsets()
        time.sleep(args.settle)
        _, lag_settle = group_offsets()
        eps = (end - start_end) / elapsed
        print(f"{rate:>12} {eps:>9.0f} {lag_stop:>9} {lag_settle:>11}")


if __name__ == "__main__":
    main()
```

#### `scripts/chaos/run_chaos.sh`

```bash
#!/usr/bin/env bash
# =============================================================================
# Chaos scenarios. Each one: run traffic, inject a failure, recover, then PROVE
# nothing was lost (verify_no_loss.py) and report duplicates (check_landing.py).
#
# Usage (stack up + connector registered, NO simulator/consumer running):
#   scripts/chaos/run_chaos.sh kill_consumer        # SIGKILL the consumer mid-stream
#   scripts/chaos/run_chaos.sh crash_before_commit  # crash after file write, before offset commit
#   scripts/chaos/run_chaos.sh stop_connect         # Debezium down 60s while writes continue
#   scripts/chaos/run_chaos.sh schema_drift         # add a source column mid-stream
#
# Logs go to chaos_logs/<scenario>-<timestamp>/. Paste the summary lines into
# the README's results table.
#
# WHY SCRIPTED (not "I tried it once by hand"): repeatable results are evidence.
# Run each scenario 3x and report the worst case.
# =============================================================================
set -euo pipefail
SCENARIO=${1:?"usage: $0 kill_consumer|crash_before_commit|stop_connect|schema_drift"}
cd "$(dirname "$0")/../.."
PY=${PY:-.venv/bin/python}
LOG="chaos_logs/${SCENARIO}-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG"

start_consumer() {   # $1 = log suffix; extra env passed through
  $PY consumer/consumer.py --flush-seconds 5 >"$LOG/consumer_$1.log" 2>&1 &
  echo $!
}

slot_lag_mb() {
  docker exec payflow-postgres psql -U payflow -d payflow -tAc \
    "SELECT ROUND(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)/1e6, 2)
     FROM pg_replication_slots WHERE slot_name='payflow_slot'"
}

echo "== $SCENARIO: starting 120s of traffic =="
$PY simulator/simulator.py --rate 50 --duration 120 --bad-rate 0.02 >"$LOG/simulator.log" 2>&1 &
SIM=$!

if [[ $SCENARIO == crash_before_commit ]]; then
  # First consumer writes its first batch, then dies before committing offsets.
  PAYFLOW_CHAOS_CRASH_BEFORE_COMMIT=1 $PY consumer/consumer.py --flush-seconds 5 >"$LOG/consumer_1.log" 2>&1 || true
  echo "consumer crashed (expected). restarting without the fault..."
  CONSUMER=$(start_consumer 2)
else
  CONSUMER=$(start_consumer 1)
  sleep 30
  case $SCENARIO in
    kill_consumer)
      kill -9 "$CONSUMER"; echo "SIGKILL consumer at $(date +%T)"
      sleep 15
      CONSUMER=$(start_consumer 2) ;;
    stop_connect)
      docker stop payflow-connect >/dev/null; echo "Debezium stopped at $(date +%T)"
      sleep 60
      echo "slot lag while Debezium was down: $(slot_lag_mb) MB" | tee -a "$LOG/summary.txt"
      docker start payflow-connect >/dev/null; t0=$(date +%s)
      # Catch-up = time until Postgres no longer holds a backlog for the slot.
      until [[ $(echo "$(slot_lag_mb) < 1" | bc) -eq 1 ]]; do sleep 2; done
      echo "Debezium caught up in $(( $(date +%s) - t0 ))s" | tee -a "$LOG/summary.txt" ;;
    schema_drift)
      docker exec payflow-postgres psql -U payflow -d payflow -c \
        "ALTER TABLE payments ADD COLUMN IF NOT EXISTS risk_score INT DEFAULT 0;" >/dev/null
      echo "added payments.risk_score at $(date +%T) (bronze must not break; silver logs drift)" ;;
  esac
fi

wait "$SIM"
echo "traffic done; draining 20s..."
sleep 20
# The consumer was started inside $(...), so it is not this shell's child and
# `wait` can't be used. Poll until the graceful shutdown (final flush) finishes.
kill -INT "$CONSUMER"
while kill -0 "$CONSUMER" 2>/dev/null; do sleep 1; done

echo "== verification =="
$PY scripts/verify_no_loss.py | tee "$LOG/verify.txt" || echo "VERIFY FAILED" | tee -a "$LOG/summary.txt"
$PY scripts/check_landing.py > "$LOG/landing.txt"
grep -A4 "Duplicate check" "$LOG/landing.txt" | tee -a "$LOG/summary.txt"
echo "logs: $LOG"
```

### Tests

#### `tests/test_consumer.py`

```python
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
```

#### `tests/test_pipeline_logic.py`

```python
"""
Unit tests for the pure logic in the uploader, reconciliation and loss check.
No Postgres, Kafka or Databricks needed.

Run:  python -m pytest tests/test_pipeline_logic.py -v
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from verify_no_loss import diff_table  # noqa: E402

from reconciliation.reconcile import compare  # noqa: E402
from uploader.upload_to_volume import upload_all  # noqa: E402


# ---------------------------------------------------------------- reconcile
def test_compare_exact_match():
    src = {"payments": [("2026-10-01", 10, 50_000)]}
    lake = {"payments": [("2026-10-01", 10, 50_000)]}
    assert all(r["match"] for r in compare(src, lake))


def test_compare_detects_missing_day_and_cent_difference():
    src = {"payments": [("2026-10-01", 10, 50_000), ("2026-10-02", 5, 100)]}
    lake = {"payments": [("2026-10-01", 10, 49_999)]}
    res = {r["key"]: r for r in compare(src, lake)}
    assert not res["2026-10-01"]["match"]          # off by one cent
    assert res["2026-10-02"]["lake_count"] == 0     # whole day missing
    assert not res["2026-10-02"]["match"]


def test_compare_detects_extra_rows_in_lake():
    res = compare({"refunds": []}, {"refunds": [("2026-10-01", 1, 10)]})
    assert len(res) == 1 and not res[0]["match"]


# ---------------------------------------------------------------- verify_no_loss
def test_diff_table_clean():
    d = diff_table({1: "captured", 2: "failed"}, {1: ("u", "captured"), 2: ("u", "failed"), 3: ("d", None)}, True)
    assert d["n_missing"] == d["n_extra"] == d["n_wrong"] == 0


def test_diff_table_finds_missed_insert_missed_delete_and_stale_update():
    pg = {1: "refunded", 2: "captured"}                       # 3 was deleted in Postgres
    landed = {1: ("u", "captured"), 3: ("c", "authorized")}    # 1 stale, 2 missing, 3 delete missed
    d = diff_table(pg, landed, check_status=True)
    assert d["missing"] == [2] and d["extra"] == [3] and d["wrong_status"] == [1]


# ---------------------------------------------------------------- uploader
class FakeFiles:
    def __init__(self, fail_on=None):
        self.uploaded, self.fail_on = [], fail_on

    def upload(self, path, f, overwrite=False):
        if self.fail_on and self.fail_on in path:
            raise ConnectionError("network down")
        self.uploaded.append((path, len(f.read()), overwrite))


class FakeClient:
    def __init__(self, files):
        self.files = files


def make_landing(tmp_path):
    landing = tmp_path / "landing"
    for t in ("payments", "refunds"):
        d = landing / t / "ingest_date=2026-10-05"
        d.mkdir(parents=True)
        (d / f"part-{t}.parquet").write_bytes(b"x" * 10)
        (d / f"part-{t}.parquet.tmp").write_bytes(b"half")   # in-progress write
    return landing


def test_upload_moves_files_and_skips_tmp(tmp_path, monkeypatch):
    monkeypatch.setenv("PAYFLOW_CATALOG", "payflow")
    landing, archive = make_landing(tmp_path), tmp_path / "archive"
    fake = FakeFiles()
    assert upload_all(FakeClient(fake), landing, archive) == 2
    paths = [p for p, _, _ in fake.uploaded if p.endswith(".parquet")]
    assert paths == sorted(paths) or len(paths) == 2
    assert all(p.startswith("/Volumes/payflow/raw/files/landing/") for p in paths)
    assert all(ow for _, _, ow in fake.uploaded)                     # idempotent re-uploads
    assert not list(landing.glob("*/*/*.parquet"))                   # moved out of landing
    assert len(list(archive.glob("*/*/*.parquet"))) == 2
    assert len(list(landing.glob("*/*/*.tmp"))) == 2                 # never touched


def test_failed_upload_leaves_file_for_retry(tmp_path):
    landing, archive = make_landing(tmp_path), tmp_path / "archive"
    try:
        upload_all(FakeClient(FakeFiles(fail_on="refunds")), landing, archive)
    except ConnectionError:
        pass
    # the refunds file is still in landing, so the next run retries it
    assert [p.parent.parent.name for p in landing.glob("*/*/*.parquet")] == ["refunds"]
```

#### `tests/test_transforms.py`

```python
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
```

#### `tests/test_dags.py`

```python
"""
DAG integrity test: every DAG file imports cleanly, has the expected tasks,
and no DAG has cycles. Catches typos before they reach the scheduler, where
a broken DAG file silently disappears from the UI.

Run:  python -m pytest tests/test_dags.py -v   (needs apache-airflow installed; skipped otherwise)
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parent.parent / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_and_tasks(dagbag):
    assert set(dagbag.dag_ids) == {
        "payflow_lakehouse", "payflow_daily_reconciliation", "payflow_maintenance", "payflow_replay",
    }
    main = dagbag.get_dag("payflow_lakehouse")
    assert [t.task_id for t in main.topological_sort()] == [
        "check_replication_slot", "upload_landing_files", "run_databricks_job", "check_freshness",
    ]
    assert main.max_active_runs == 1 and main.catchup is False


def test_replay_passes_full_refresh(dagbag):
    op = dagbag.get_dag("payflow_replay").get_task("full_refresh_from_bronze")
    assert op.json["job_parameters"]["full_refresh"] == "true"
```

### Phase 7: CI

#### `.github/workflows/ci.yml`

```yaml
# =============================================================================
# CI: three jobs, cheapest first.
#   unit  - lint + unit tests + Spark transform tests      (~2 min)
#   dags  - Airflow DAG integrity against real Airflow     (~3 min)
#   e2e   - real Postgres + Kafka + Debezium, 60s of traffic, prove zero loss
#
# WHY AN E2E JOB: unit tests can't catch a wrong Debezium setting or a broken
# publication. Spinning up the real capture path on every push and checking
# "every Postgres row arrived with its latest state" catches exactly that.
# Databricks isn't called from CI (would need secrets and burn free quota);
# transforms are covered by the local Spark tests instead.
# =============================================================================
name: ci
on:
  push:
  pull_request:

jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11", cache: pip }
      - uses: actions/setup-java@v4          # local Spark for transform tests
        with: { distribution: temurin, java-version: "17" }
      - run: pip install -r requirements.txt -r requirements-dev.txt
      - run: ruff check .
      - run: pytest tests/test_consumer.py tests/test_pipeline_logic.py tests/test_transforms.py -v

  dags:
    runs-on: ubuntu-latest
    env:
      AIRFLOW_HOME: /tmp/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      PYTHONPATH: ${{ github.workspace }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      # Constraints file = the exact dependency set Airflow 2.10.5 was tested with.
      - run: >
          pip install "apache-airflow==2.10.5" apache-airflow-providers-databricks "psycopg[binary]" pytest
          --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
      - run: airflow db migrate
      - run: pytest tests/test_dags.py -v

  e2e:
    needs: unit
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11", cache: pip }
      - run: pip install -r requirements.txt
      - name: Start capture stack (no Airflow / UI needed)
        run: |
          cp .env.example .env
          docker compose up -d --wait postgres kafka connect
      - run: python connectors/register_connector.py
      - name: Traffic + consumer, then prove zero loss
        run: |
          python consumer/consumer.py --flush-seconds 5 > consumer.log 2>&1 &
          CONSUMER=$!
          python simulator/simulator.py --rate 50 --duration 60 --seed 1
          sleep 25                                   # let Debezium + consumer drain
          kill -INT $CONSUMER; wait $CONSUMER || true
          python scripts/verify_no_loss.py
          python scripts/check_landing.py
      - if: failure()
        run: |
          cat consumer.log || true
          docker compose logs connect | tail -100
```

