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

-- 1. Login role with the REPLICATION attribute (needed to open a replication slot).
-- NOTE: plain-text password is fine for local Docker only. In a real setup this
-- would come from a secrets manager.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';

-- 2. Read access. SELECT is needed for the initial snapshot (Debezium reads
--    existing rows once before switching to streaming the WAL).
GRANT CONNECT ON DATABASE payflow TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- 3. Heartbeat table (edge case: quiet source, busy server).
--    The replication slot only advances when Debezium confirms an LSN, and it
--    only confirms LSNs of changes it receives. If the 5 payment tables are
--    idle while something else on this server writes WAL (Airflow's metadata
--    database lives here too), the slot holds that WAL forever and the slot-lag
--    alert fires for no reason. Debezium's `heartbeat.action.query` upserts one
--    row here every heartbeat, which produces a tiny captured change and lets
--    the slot move. The table is in the publication but NOT in the connector's
--    table.include.list, so it never becomes a Kafka topic.
CREATE TABLE debezium_heartbeat (
    id  INTEGER     PRIMARY KEY,
    ts  TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT INSERT, UPDATE ON debezium_heartbeat TO debezium;

-- 4. Publication. pgoutput is Postgres's built-in logical decoding plugin: no
--    extensions to install, works on RDS/Cloud SQL too. (The older
--    wal2json/decoderbufs plugins need extra installs.)
CREATE PUBLICATION payflow_pub FOR TABLE
    merchants, customers, payments, refunds, disputes, debezium_heartbeat;
