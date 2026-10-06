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
    the pipeline, not the insights.

WHY GROUND-TRUTH LOGGING of injected bad records:
    Every bad row we inject is written to simulator/injected/bad_records.jsonl.
    Later we join that against what the quality checks flagged, which gives a
    real "caught X% of bad records" number for the resume, instead of a guess.

Edge cases handled:
    1. Concurrent simulators (the benchmark runs several). Every state change is
       guarded (`UPDATE ... WHERE id = %s AND status = <expected>`), so two
       processes racing on one payment can't move it backwards, e.g.
       refunded -> failed. Without the guard a CLEAN refund would end up on a
       failed payment, and the DQ rules would report a false positive.
    2. Refunds are inserted only after the payment's status change succeeded,
       so a good refund never lands on a payment someone else just changed.
    3. Emails: Faker's `unique` proxy raises after it runs out of values on long
       runs. We append a counter instead (emails need not be unique in the DB).
    4. Postgres restarts: a broken connection is re-opened with backoff instead
       of crashing the simulator.
    5. Bad CLI values (--rate 0, --bad-rate 2) are rejected up front.
"""

from __future__ import annotations

import argparse
import itertools
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
BAD_LOG = Path(os.getenv("GROUND_TRUTH", Path(__file__).parent / "injected" / "bad_records.jsonl"))

# --- 1. Reference values ------------------------------------------------------
CATEGORIES = ["electronics", "travel", "food_delivery", "apparel", "saas", "gaming", "health"]
COUNTRIES = ["US", "US", "US", "GB", "DE", "IN", "CA", "FR", "BR"]  # repeated = weighted
CURRENCIES = ["USD"] * 8 + ["EUR", "GBP"]
CARD_BRANDS = ["visa", "visa", "mastercard", "amex", "discover"]
FAILURE_REASONS = ["insufficient_funds", "card_declined", "expired_card", "fraud_suspected"]
REFUND_REASONS = ["requested_by_customer", "duplicate", "fraudulent"]
DISPUTE_REASONS = ["fraudulent", "product_not_received", "unrecognized", "duplicate"]
RISK_TIERS = ["low", "medium", "high"]

# --- 2. Action mix ------------------------------------------------------------
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
        # Per-process suffix so several simulators never generate the same email.
        self._email_seq = itertools.count()
        self._email_tag = f"{os.getpid():x}"
        BAD_LOG.parent.mkdir(parents=True, exist_ok=True)
        self.bad_log = BAD_LOG.open("a", encoding="utf-8")

    # ------------------------------------------------------------ 3. helpers
    def is_bad(self) -> bool:
        return self.rng.random() < self.bad_rate

    def log_bad(self, table: str, pk: int, kind: str) -> None:
        """
        Record ground truth for every injected bad row.
        Called only AFTER the transaction commits (see step()), so the log
        never claims a bad row that was rolled back.
        """
        self.bad_log.write(json.dumps({
            "injected_at": datetime.now(timezone.utc).isoformat(),
            "table": table, "pk": pk, "kind": kind,
        }) + "\n")
        self.bad_log.flush()
        self.stats[f"bad:{kind}"] += 1

    def email(self) -> str:
        user, domain = self.fake.user_name(), self.fake.free_email_domain()
        return f"{user}.{self._email_tag}{next(self._email_seq)}@{domain}"

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
            return None  # edge case: empty table (first seconds of a fresh DB)
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

    def transition(self, cur, table: str, pk: str, pk_value: int, expected: str, sets: str, params=()) -> bool:
        """
        Guarded state change: only applies if the row is still in `expected`
        status. Returns False if another process changed it first (edge case 1).
        """
        cur.execute(f"UPDATE {table} SET {sets} WHERE {pk} = %s AND status = %s",
                    (*params, pk_value, expected))
        return cur.rowcount == 1

    # --------------------------------------------------------- 4. bootstrap
    def bootstrap(self, n_merchants: int, n_customers: int) -> None:
        """Create the initial merchants/customers once (skipped if they exist)."""
        with self.conn.cursor() as cur:
            # Advisory lock: if several simulators start on an empty DB at the
            # same moment (benchmark), only one bootstraps.
            cur.execute("SELECT pg_advisory_xact_lock(4242)")
            cur.execute("SELECT COUNT(*) FROM merchants")
            if cur.fetchone()[0] >= n_merchants:
                print("Bootstrap skipped: data already present.")
                self.conn.commit()
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
                [(self.email(), self.rng.choice(COUNTRIES)) for _ in range(n_customers)],
            )
        self.conn.commit()

    # ----------------------------------------------------------- 5. actions
    # Each action = one small transaction, like a real app request.
    # WHY ONE TRANSACTION PER ACTION: it matches how an OLTP app behaves and
    # gives Debezium realistic, small transactions. Tradeoff: lower write
    # throughput than batching, but we're simulating an app, not bulk-loading.
    # Each action RETURNS the bad records it created as [(table, pk, kind)];
    # step() logs them only after the commit succeeds.

    def new_payment(self, cur) -> list:
        merchant = self.pick_recent(cur, "merchants", "merchant_id", "status = 'active'", window=10**9)
        customer = self.pick_recent(cur, "customers", "customer_id", "TRUE", window=10**9)
        if not merchant or not customer:
            return []
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
        return [("payments", pid, bad_kind)] if bad_kind else []

    def capture(self, cur) -> list:
        row = self.pick_recent(cur, "payments", "payment_id", "status = 'authorized'", window=2000)
        if row:
            self.transition(cur, "payments", "payment_id", row[0], "authorized", "status = 'captured'")
        return []

    def fail_or_void(self, cur) -> list:
        row = self.pick_recent(cur, "payments", "payment_id", "status = 'authorized'", window=2000)
        if not row:
            return []
        if self.rng.random() < 0.7:
            self.transition(cur, "payments", "payment_id", row[0], "authorized",
                            "status = 'failed', failure_reason = %s", (self.rng.choice(FAILURE_REASONS),))
        else:
            self.transition(cur, "payments", "payment_id", row[0], "authorized", "status = 'voided'")
        return []

    def new_refund(self, cur) -> list:
        # Payment row columns: payment_id, merchant_id, customer_id, amount_cents, ...
        if self.is_bad():
            # Business-rule violations the DB happily accepts.
            kind = self.rng.choice(["refund_exceeds_payment", "refund_on_failed_payment"])
            where = "status = 'captured'" if kind == "refund_exceeds_payment" else "status = 'failed'"
            row = self.pick_recent(cur, "payments", "payment_id", where)
            if not row:
                return []
            amt = abs(row[3]) * 2 + 100 if kind == "refund_exceeds_payment" else max(abs(row[3]), 100)
            cur.execute("INSERT INTO refunds (payment_id, amount_cents, reason) VALUES (%s, %s, %s) RETURNING refund_id",
                        (row[0], amt, self.rng.choice(REFUND_REASONS)))
            return [("refunds", cur.fetchone()[0], kind)]

        row = self.pick_recent(cur, "payments", "payment_id", "status = 'captured' AND amount_cents > 0")
        if not row:
            return []
        full = self.rng.random() < 0.7
        amt = row[3] if full else max(1, int(row[3] * self.rng.uniform(0.1, 0.9)))
        # Edge case 2: claim the payment first; only refund if we won the race.
        if self.transition(cur, "payments", "payment_id", row[0], "captured", "status = %s",
                           ("refunded" if full else "partially_refunded",)):
            cur.execute("INSERT INTO refunds (payment_id, amount_cents, reason) VALUES (%s, %s, %s)",
                        (row[0], amt, self.rng.choice(REFUND_REASONS)))
        return []

    def settle_refund(self, cur) -> list:
        row = self.pick_recent(cur, "refunds", "refund_id", "status = 'pending'", window=2000)
        if row:
            new_status = "succeeded" if self.rng.random() < 0.95 else "failed"
            self.transition(cur, "refunds", "refund_id", row[0], "pending", "status = %s", (new_status,))
        return []

    def open_dispute(self, cur) -> list:
        row = self.pick_recent(cur, "payments", "payment_id", "status = 'captured' AND amount_cents > 0")
        if row:
            cur.execute("INSERT INTO disputes (payment_id, amount_cents, reason) VALUES (%s, %s, %s)",
                        (row[0], row[3], self.rng.choice(DISPUTE_REASONS)))
        return []

    def resolve_dispute(self, cur) -> list:
        row = self.pick_recent(cur, "disputes", "dispute_id", "status = 'open'", window=10**9)
        if row:
            self.transition(cur, "disputes", "dispute_id", row[0], "open", "status = %s",
                            ("won" if self.rng.random() < 0.4 else "lost",))
        return []

    def new_customer(self, cur) -> list:
        cur.execute("INSERT INTO customers (email, country) VALUES (%s, %s)",
                    (self.email(), self.rng.choice(COUNTRIES)))
        return []

    def update_merchant(self, cur) -> list:
        # Risk tier and fee changes are what the SCD2 merchant dimension tracks.
        row = self.pick_recent(cur, "merchants", "merchant_id", "TRUE", window=10**9)
        if not row:
            return []
        if self.rng.random() < 0.6:
            cur.execute("UPDATE merchants SET risk_tier = %s WHERE merchant_id = %s",
                        (self.rng.choice(RISK_TIERS), row[0]))
        else:
            cur.execute("UPDATE merchants SET fee_bps = %s WHERE merchant_id = %s",
                        (self.rng.choice([250, 290, 320, 350]), row[0]))
        return []

    def new_merchant(self, cur) -> list:
        country = self.rng.choice(COUNTRIES)
        bad = self.is_bad()
        cur.execute(
            "INSERT INTO merchants (name, category, country) VALUES (%s, %s, %s) RETURNING merchant_id",
            (self.fake.company(), self.rng.choice(CATEGORIES), None if bad else country),
        )
        mid = cur.fetchone()[0]
        return [("merchants", mid, "null_country")] if bad else []

    def delete_customer(self, cur) -> list:
        # Hard delete. FK is ON DELETE SET NULL, so Postgres also UPDATEs that
        # customer's payments; CDC captures those cascaded updates too.
        row = self.pick_recent(cur, "customers", "customer_id", "TRUE", window=10**9)
        if row:
            cur.execute("DELETE FROM customers WHERE customer_id = %s", (row[0],))
        return []

    # -------------------------------------------------------------- 6. loop
    def step(self) -> None:
        """Pick one weighted action, run it in its own transaction, log bad rows after commit."""
        name = self.rng.choices(list(ACTIONS), weights=list(ACTIONS.values()))[0]
        try:
            with self.conn.cursor() as cur:
                bad = getattr(self, name)(cur)
            self.conn.commit()
            for table, pk, kind in bad:
                self.log_bad(table, pk, kind)
            self.stats[name] += 1
        except psycopg.OperationalError:
            # Edge case 4: connection-level failure. Let main() reconnect.
            raise
        except psycopg.Error as e:
            # Don't crash the whole simulator over one failed action (e.g. a
            # race on a row another action just changed). Roll back and move on.
            self.conn.rollback()
            self.stats["errors"] += 1
            if self.stats["errors"] <= 5:
                print(f"[warn] {name} failed: {e}")

    def close(self) -> None:
        self.bad_log.close()


def connect_with_retry(attempts: int = 10) -> psycopg.Connection:
    """Open a connection, retrying with backoff (Postgres may still be starting/restarting)."""
    for i in range(attempts):
        try:
            return psycopg.connect(PG_DSN, connect_timeout=5)
        except psycopg.OperationalError as e:
            wait = min(2 ** i, 15)
            print(f"[warn] Postgres not reachable ({e.__class__.__name__}); retry in {wait}s")
            time.sleep(wait)
    raise SystemExit(f"Postgres not reachable at {PG_DSN}")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PayFlow payments traffic simulator")
    p.add_argument("--rate", type=float, default=20, help="actions per second (default 20)")
    p.add_argument("--duration", type=int, default=0, help="seconds to run, 0 = until Ctrl+C")
    p.add_argument("--bad-rate", type=float, default=0.02, help="probability an eligible action injects bad data")
    p.add_argument("--merchants", type=int, default=200)
    p.add_argument("--customers", type=int, default=5000)
    p.add_argument("--seed", type=int, default=None, help="set for reproducible runs")
    args = p.parse_args(argv)
    # Edge case 5: reject values that would divide by zero or make no sense.
    if args.rate <= 0:
        p.error("--rate must be > 0")
    if not 0.0 <= args.bad_rate <= 1.0:
        p.error("--bad-rate must be between 0 and 1")
    if args.duration < 0 or args.merchants < 1 or args.customers < 1:
        p.error("--duration must be >= 0; --merchants and --customers must be >= 1")
    return args


def main() -> None:
    args = parse_args()
    stop = False

    def handle_sigint(*_):
        nonlocal stop
        stop = True

    # 1. Ctrl+C / SIGTERM finish the current action, then print final counts.
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    # 2. Connect and seed the reference data once.
    conn = connect_with_retry()
    sim = Simulator(conn, bad_rate=args.bad_rate, seed=args.seed)
    sim.bootstrap(args.merchants, args.customers)

    interval = 1.0 / args.rate
    started = last_report = time.monotonic()
    print(f"Simulating at ~{args.rate}/s, bad-rate={args.bad_rate}. Ctrl+C to stop.")

    # 3. Main loop: one action per tick, paced to --rate.
    try:
        while not stop:
            t0 = time.monotonic()
            if args.duration and t0 - started >= args.duration:
                break
            try:
                sim.step()
            except psycopg.OperationalError as e:
                print(f"[warn] connection lost ({e}); reconnecting")
                try:
                    sim.conn.close()
                except psycopg.Error:
                    pass
                sim.conn = connect_with_retry()
                sim.stats["reconnects"] += 1
                continue
            if t0 - last_report >= 10:
                actions = sum(v for k, v in sim.stats.items() if k in ACTIONS)
                print(f"[{int(t0 - started)}s] actions={actions} "
                      f"bad={sum(v for k, v in sim.stats.items() if k.startswith('bad:'))} "
                      f"errors={sim.stats['errors']}")
                last_report = t0
            # Simple pacing. Tradeoff: not exact under load, but good enough.
            time.sleep(max(0.0, interval - (time.monotonic() - t0)))
    finally:
        # 4. Always close the ground-truth file and the connection.
        sim.close()
        sim.conn.close()
        print("\nFinal counts:")
        for k, v in sorted(sim.stats.items()):
            print(f"  {k:24s} {v}")


if __name__ == "__main__":
    main()
