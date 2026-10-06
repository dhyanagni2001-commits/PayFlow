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
# the README's results table. Exit code: 0 = verify OK, 1 = data lost.
#
# WHY SCRIPTED (not "I tried it once by hand"): repeatable results are evidence.
# Run each scenario 3x and report the worst case.
#
# Steps: 1. start traffic  2. start consumer  3. inject the failure  4. recover
#        5. wait for traffic to end and drain  6. verify + count duplicates
#
# Edge cases handled:
#   - Ctrl+C or an error mid-scenario kills the simulator/consumer we started
#     and restarts Debezium if we stopped it (trap on EXIT)
#   - Debezium catch-up wait is bounded (180 s), never an infinite loop
#   - a consumer that already died doesn't abort the script before verification
#   - unknown scenario names are rejected before anything starts
# =============================================================================
set -euo pipefail
SCENARIO=${1:?"usage: $0 kill_consumer|crash_before_commit|stop_connect|schema_drift"}
case $SCENARIO in
  kill_consumer|crash_before_commit|stop_connect|schema_drift) ;;
  *) echo "unknown scenario: $SCENARIO" >&2; exit 2 ;;
esac
cd "$(dirname "$0")/../.."
PY=${PY:-.venv/bin/python}
LOG="chaos_logs/${SCENARIO}-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG"
SIM="" CONSUMER=""

cleanup() {
  [[ -n $SIM ]] && kill "$SIM" 2>/dev/null || true
  [[ -n $CONSUMER ]] && kill -INT "$CONSUMER" 2>/dev/null || true
  docker start payflow-connect >/dev/null 2>&1 || true
}
trap cleanup EXIT

start_consumer() {   # $1 = log suffix; extra env passed through
  $PY consumer/consumer.py --flush-seconds 5 >"$LOG/consumer_$1.log" 2>&1 &
  echo $!
}

slot_lag_mb() {
  docker exec payflow-postgres psql -U payflow -d payflow -tAc \
    "SELECT ROUND(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)/1e6, 2)
     FROM pg_replication_slots WHERE slot_name='payflow_slot'"
}

# 1. Traffic.
echo "== $SCENARIO: starting 120s of traffic =="
$PY simulator/simulator.py --rate 50 --duration 120 --bad-rate 0.02 >"$LOG/simulator.log" 2>&1 &
SIM=$!

# 2-4. Consumer + failure + recovery.
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
      until [[ $(echo "$(slot_lag_mb) < 1" | bc) -eq 1 ]]; do
        if (( $(date +%s) - t0 > 180 )); then
          echo "Debezium did NOT catch up within 180s" | tee -a "$LOG/summary.txt"; break
        fi
        sleep 2
      done
      echo "Debezium caught up in $(( $(date +%s) - t0 ))s" | tee -a "$LOG/summary.txt" ;;
    schema_drift)
      docker exec payflow-postgres psql -U payflow -d payflow -c \
        "ALTER TABLE payments ADD COLUMN IF NOT EXISTS risk_score INT DEFAULT 0;" >/dev/null
      echo "added payments.risk_score at $(date +%T) (bronze must not break; silver logs drift)" ;;
  esac
fi

# 5. Let traffic finish, then drain.
wait "$SIM" || true
SIM=""
echo "traffic done; draining 20s..."
sleep 20
# The consumer was started inside $(...), so it is not this shell's child and
# `wait` can't be used. Poll until the graceful shutdown (final flush) finishes.
kill -INT "$CONSUMER" 2>/dev/null || echo "consumer was not running (died?)" | tee -a "$LOG/summary.txt"
while kill -0 "$CONSUMER" 2>/dev/null; do sleep 1; done
CONSUMER=""

# 6. Proof.
echo "== verification =="
status=0
$PY scripts/verify_no_loss.py | tee "$LOG/verify.txt" || { status=1; echo "VERIFY FAILED" | tee -a "$LOG/summary.txt"; }
$PY scripts/check_landing.py > "$LOG/landing.txt"
grep -A6 "Duplicate check" "$LOG/landing.txt" | tee -a "$LOG/summary.txt"
echo "logs: $LOG"
exit $status
