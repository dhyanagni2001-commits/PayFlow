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

Steps in main():
    1. wait until the Kafka Connect REST API answers
    2. PUT the config (create or update)
    3. poll status until connector AND task are RUNNING (exit 1 on FAILED/timeout)

Edge cases handled:
    - Connect still booting           -> step 1 retries for up to 120 s
    - Connect returns 409 (rebalance) -> step 2 retries a few times
    - task FAILED (bad password, missing publication, slot in use)
                                      -> prints the Java stack trace, exits 1
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

    # --- 1. Source connection -----------------------------------------------
    # "postgres" is the Docker service name; Connect runs inside the Docker
    # network, so it doesn't use localhost.
    "database.hostname": os.getenv("DBZ_DB_HOST", "postgres"),
    "database.port": "5432",
    "database.user": "debezium",          # least-privilege user from 02_cdc_setup.sql
    "database.password": os.getenv("DBZ_DB_PASSWORD", "debezium"),
    "database.dbname": "payflow",

    # --- 2. Logical decoding ------------------------------------------------
    # pgoutput = built into Postgres, nothing to install.
    "plugin.name": "pgoutput",
    # We created the publication ourselves (see 02_cdc_setup.sql), so Debezium
    # must not try to create/alter it.
    "publication.name": "payflow_pub",
    "publication.autocreate.mode": "disabled",
    # The replication slot is Postgres's bookmark of how far Debezium has read.
    # Postgres keeps WAL until the slot confirms it. RISK: if the connector is
    # down for days, WAL piles up and can fill the source disk. The Airflow DAG
    # checks slot lag every run for exactly this reason.
    "slot.name": "payflow_slot",

    # --- 3. What to capture -------------------------------------------------
    # Topic names become: payflow.public.payments, payflow.public.refunds, ...
    # debezium_heartbeat is in the publication but deliberately NOT listed
    # here, so its changes advance the slot without creating a topic.
    "topic.prefix": "payflow",
    "table.include.list": (
        "public.merchants,public.customers,public.payments,"
        "public.refunds,public.disputes"
    ),

    # --- 4. Snapshot --------------------------------------------------------
    # "initial": on first start, read every existing row once (op = "r"), then
    # stream changes. Without it, the lakehouse would miss rows created before
    # the connector existed.
    "snapshot.mode": "initial",

    # --- 5. Message format --------------------------------------------------
    # JSON without embedded schemas.
    #   + human-readable in Kafka UI, no Schema Registry container to run
    #   - bigger messages; no enforced schema contract between producer and
    #     consumer (Avro + Schema Registry gives you that)
    # With a single consuming team, readability wins. With several teams,
    # production would use Avro/Protobuf + a registry.
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

    # --- 6. Health ----------------------------------------------------------
    # Heartbeat every 10 s. The action query writes a captured change, so the
    # slot advances even when the payment tables are quiet but other databases
    # on the same server (Airflow metadata) keep producing WAL.
    "heartbeat.interval.ms": "10000",
    "heartbeat.action.query": (
        "INSERT INTO debezium_heartbeat (id, ts) VALUES (1, now()) "
        "ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
    ),
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
    """Step 1: Kafka Connect takes a while to boot; poll until its REST API answers."""
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


def task_states(status: dict) -> tuple[str, list[str]]:
    """Pure helper (unit tested): (connector state, [task states]) from a /status body."""
    if not isinstance(status, dict):
        return "UNKNOWN", []
    conn = status.get("connector", {}).get("state", "UNKNOWN")
    return conn, [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]


def wait_until_running(timeout_s: int = 90) -> dict:
    """Step 3: poll until connector + every task is RUNNING. Exit 1 on FAILED or timeout."""
    deadline = time.time() + timeout_s
    state: dict | str = {}
    while time.time() < deadline:
        _, state = request("GET", f"/connectors/{CONNECTOR_NAME}/status")
        conn, tasks = task_states(state)
        if "FAILED" in (conn, *tasks):
            print(json.dumps(state, indent=2))
            sys.exit("Connector or task FAILED. The 'trace' above explains why "
                     "(common: wrong password, publication missing, slot already active).")
        if conn == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            return state
        time.sleep(2)
    print(json.dumps(state, indent=2))
    sys.exit(f"Connector not RUNNING after {timeout_s}s")


def main() -> None:
    # 1. Connect must be up before we can talk to it.
    wait_for_connect()

    # 2. Create-or-update. 409 = Connect is mid-rebalance (common right after
    #    boot); retrying a few seconds later succeeds.
    for attempt in range(1, 6):
        status, body = request("PUT", f"/connectors/{CONNECTOR_NAME}/config", CONFIG)
        if status in (200, 201):
            break
        if status == 409 and attempt < 5:
            print(f"Connect is rebalancing (409), retry {attempt}/4...")
            time.sleep(3)
            continue
        sys.exit(f"Failed to register connector ({status}): {body}")
    print(f"Connector '{CONNECTOR_NAME}' registered (HTTP {status}).")

    # 3. Don't trust "registered": a task can still fail on start-up. Wait for
    #    a real RUNNING state (prints "state": "RUNNING" twice: connector + task).
    print(json.dumps(wait_until_running(), indent=2))


if __name__ == "__main__":
    main()
