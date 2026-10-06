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

Steps per rate: 1. read end offsets  2. run N simulators  3. read lag at stop
4. wait --settle seconds  5. read lag again  6. print one row

Edge case handled: a partition with no committed offset yet (CURRENT-OFFSET
"-") counts as fully lagging (lag = log-end offset), not as zero lag.
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


def parse_group_describe(out: str) -> tuple[int, int]:
    """Pure parser (unit tested): (sum of log-end offsets, sum of lag) for payflow topics."""
    end = lag = 0
    for line in out.splitlines():
        parts = re.split(r"\s+", line.strip())
        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(parts) >= 6 and parts[1].startswith("payflow.public.") and parts[4].isdigit():
            log_end = int(parts[4])
            end += log_end
            # "-" = nothing committed yet: everything in the partition is lag.
            lag += int(parts[5]) if parts[5].isdigit() else log_end
    return end, lag


def group_offsets() -> tuple[int, int]:
    """(sum of log-end offsets, sum of lag) across payflow topics for the consumer group."""
    out = subprocess.run(
        ["docker", "exec", "payflow-kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
         "--bootstrap-server", "localhost:9092", "--describe", "--group", GROUP],
        capture_output=True, text=True, check=True).stdout
    return parse_group_describe(out)


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
