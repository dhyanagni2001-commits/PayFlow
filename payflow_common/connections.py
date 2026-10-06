"""
Connection helpers shared by the uploader, reconciliation, and Airflow DAGs.
All settings come from environment variables (see .env.example), never code.

WHY ENV VARS: the same code runs on your laptop, inside the Airflow container,
and in CI. Only the environment changes. Secrets stay out of git.

Edge cases handled:
  1. Missing Databricks settings -> one clear error naming every missing
     variable, instead of a KeyError deep inside a task.
  2. Host given with or without https:// and trailing slash.
  3. Postgres session time zone pinned to UTC, so DATE() buckets match the
     Databricks side no matter what the server or laptop time zone is.
  4. Values are passed as query PARAMETERS (never pasted into SQL), so JSON
     with quotes or backslashes can't break or inject into a statement.
"""

import os

import psycopg

DBX_REQUIRED = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH")


def catalog() -> str:
    """Unity Catalog name. 'workspace' if Free Edition won't let you create 'payflow'."""
    return os.getenv("PAYFLOW_CATALOG") or "payflow"


def pg_connect() -> psycopg.Connection:
    """1. Source Postgres (read-only usage in reconciliation), session pinned to UTC."""
    conn = psycopg.connect(os.getenv("PG_DSN", "postgresql://payflow:payflow@localhost:5432/payflow"),
                           connect_timeout=10)
    conn.execute("SET TIME ZONE 'UTC'")
    return conn


def check_dbx_env() -> None:
    """2. Fail early and clearly if .env isn't filled in."""
    missing = [k for k in DBX_REQUIRED if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Databricks not configured: set {', '.join(missing)} in .env (see .env.example)")


def dbx_sql_connect():
    """
    3. Databricks SQL warehouse connection (the one 2X-Small warehouse in Free Edition).
    Imported lazily so unit tests don't need the package.
    """
    check_dbx_env()
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    return sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        # Same time zone as Postgres-side queries, so DATE() buckets match.
        session_configuration={"timezone": "UTC"},
    )


def dbx_query(sql_text: str, params: dict | None = None) -> list[tuple]:
    """4. Run one statement; `params` are bound as :name markers (native parameters)."""
    with dbx_sql_connect() as conn, conn.cursor() as cur:
        cur.execute(sql_text, params) if params else cur.execute(sql_text)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []


def dbx_query_dicts(sql_text: str) -> list[dict]:
    """Same as dbx_query, but rows as {column_name: value}. Use when column order isn't guaranteed."""
    with dbx_sql_connect() as conn, conn.cursor() as cur:
        cur.execute(sql_text)
        if not cur.description:
            return []
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
