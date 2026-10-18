CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    stage           TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    items           JSONB NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    post_timeout_ms INTEGER NOT NULL DEFAULT 2500,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_due_idx ON orders (next_attempt_at)
WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS order_paid_outbox (
    event_id        UUID PRIMARY KEY,
    order_id        UUID NOT NULL UNIQUE REFERENCES orders(id) ON DELETE CASCADE,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS order_paid_outbox_due_idx
    ON order_paid_outbox (next_attempt_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_delivery_events (
    event_id     TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    bonus_points BIGINT NOT NULL CHECK (bonus_points > 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bonus_balances (
    user_id    TEXT PRIMARY KEY,
    points     BIGINT NOT NULL CHECK (points >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
