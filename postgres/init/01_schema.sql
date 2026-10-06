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

-- ---------------------------------------------------------------- 1. merchants
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

-- ---------------------------------------------------------------- 2. customers
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

-- ---------------------------------------------------------------- 3. payments
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

-- ---------------------------------------------------------------- 4. refunds
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

-- ---------------------------------------------------------------- 5. disputes
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

-- ---------------------------------------------------------------- 6. indexes
-- Indexes the simulator needs (it looks up payments by status).
CREATE INDEX idx_payments_status   ON payments(status);
CREATE INDEX idx_payments_merchant ON payments(merchant_id);
CREATE INDEX idx_refunds_payment   ON refunds(payment_id);
CREATE INDEX idx_disputes_payment  ON disputes(payment_id);

-- -----------------------------------------------------------------------------
-- 7. updated_at trigger (one per table)
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
-- 8. REPLICA IDENTITY FULL
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
