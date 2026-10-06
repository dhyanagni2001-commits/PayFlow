# Shortcuts. GitHub Actions calls the same commands, so local == CI.
# Numbered in the order you run them the first time:
#   1 install  2 up  3 register  4 simulate  5 consume  6 check  7 verify
#   8 test     9 local-lakehouse  10 deploy  11 upload  12 airflow-up  13 reconcile
#  14 chaos   15 bench            16 down    17 reset
.PHONY: install up register simulate consume check verify test test-transforms local-lakehouse \
        deploy upload airflow-up reconcile chaos bench down reset env-check

PYTHON ?= python3
# Java 17 for local Spark (Homebrew keg-only path on macOS; ignored if absent).
JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || ls -d /opt/homebrew/opt/openjdk@17 2>/dev/null)
export JAVA_HOME

install:          ## 1. venv + all Python deps (needs Python 3.11+)
	@$(PYTHON) -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else "Python 3.11+ required: run make install PYTHON=python3.11")'
	$(PYTHON) -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt

up:               ## 2. Postgres, Kafka, Debezium, Kafka UI (Phase 1 stack)
	docker compose up -d --wait postgres kafka connect kafka-ui

register:         ## 3. register Debezium connector (idempotent; waits for RUNNING)
	.venv/bin/python connectors/register_connector.py

simulate:         ## 4. payments traffic (Ctrl+C to stop)
	.venv/bin/python simulator/simulator.py --rate 20 --bad-rate 0.02

consume:          ## 5. Kafka -> Parquet landing (Ctrl+C to stop)
	.venv/bin/python consumer/consumer.py

check:            ## 6. landing metrics: counts, duplicates, latency, file sizes
	.venv/bin/python scripts/check_landing.py

verify:           ## 7. prove zero loss: Postgres vs landed events (stop simulator first)
	.venv/bin/python scripts/verify_no_loss.py

test:             ## 8. lint + all local tests (Spark tests need Java 17)
	.venv/bin/ruff check . && .venv/bin/pytest tests -v

test-transforms:  ## 8b. only the local Spark transform tests
	.venv/bin/pytest tests/test_transforms.py -v

local-lakehouse:  ## 9. run silver/quality/gold logic on local Spark over landed files (no Databricks)
	.venv/bin/python scripts/local_lakehouse.py

env-check:
	@test -f .env || { echo ".env missing: cp .env.example .env and fill in the Databricks values"; exit 1; }

deploy: env-check ## 10. push notebooks + create/update the Databricks job
	set -a && . ./.env && set +a && .venv/bin/python databricks/deploy.py

upload: env-check ## 11. manual upload of landing files to the Databricks Volume
	set -a && . ./.env && set +a && .venv/bin/python uploader/upload_to_volume.py

airflow-up:       ## 12. start Airflow (http://localhost:8080, password in container logs)
	docker compose up -d airflow

reconcile: env-check ## 13. quiesced full reconciliation (stop simulator, wait for a pipeline run)
	set -a && . ./.env && set +a && .venv/bin/python reconciliation/reconcile.py --mode full

chaos:            ## 14. all chaos scenarios
	for s in kill_consumer crash_before_commit stop_connect schema_drift; do scripts/chaos/run_chaos.sh $$s || exit 1; done

bench:            ## 15. throughput benchmark (consumer must be running)
	.venv/bin/python scripts/benchmark_throughput.py --rates 50 100 200 400 --procs 4

down:             ## 16. stop containers (keeps Postgres data)
	docker compose down

reset:            ## 17. wipe everything local: containers, volumes, landing files
	docker compose down -v && rm -rf landing landing_archive simulator/injected chaos_logs local_lakehouse
