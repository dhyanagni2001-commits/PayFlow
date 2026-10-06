-- =============================================================================
-- Separate database + user for Airflow's metadata (Phase 5).
-- Lives on the same Postgres server to save RAM, but in its own database, and
-- it's NOT in the payflow_pub publication, so Debezium never captures it.
--
-- NOTE: init scripts run only on an EMPTY data volume. If you already ran
-- Phase 1, either `make reset` or run this file by hand:
--   docker exec -it payflow-postgres psql -U payflow -d payflow -f /docker-entrypoint-initdb.d/03_airflow_db.sql
--
-- Edge case handled: the script is IDEMPOTENT, so running it by hand on a
-- volume where it already ran is a no-op instead of an error.
-- =============================================================================

-- 1. Role, only if missing (CREATE ROLE has no IF NOT EXISTS).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
    END IF;
END
$$;

-- 2. Database, only if missing. CREATE DATABASE can't run inside a DO block
--    (it refuses to run in a transaction), so psql's \gexec runs the generated
--    statement only when the SELECT returns a row.
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')\gexec
