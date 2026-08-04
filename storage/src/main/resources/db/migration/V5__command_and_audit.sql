-- The command path and the audit log.
--
-- Verified against PostgreSQL 17.

-- ------------------------------------------------------------------ commands --

-- A command's life: PENDING once accepted, SENT once the relay has published it,
-- ACKED / REJECTED once the vehicle answers, TIMED_OUT if it never does.
--
-- The vehicle's answer is kept apart from the platform's own state on purpose.
-- REJECTED means the vehicle understood the command and refused it, which is a
-- successful round trip and a failed command; TIMED_OUT means nobody knows what
-- happened. Collapsing them into one "failed" state would hide the difference
-- between a command that was declined and a command that may yet execute.
CREATE TABLE command (
    id               UUID        PRIMARY KEY,
    tenant_id        TEXT        NOT NULL REFERENCES tenant (tenant_id) ON DELETE CASCADE,
    device_id        TEXT        NOT NULL,

    -- Supplied by the caller. Retrying a request with the same key returns the
    -- original command rather than issuing a second one -- which for ARM or
    -- TAKEOFF is the difference between a retry and a second launch.
    idempotency_key  TEXT        NOT NULL,

    command_type     TEXT        NOT NULL,
    params           JSONB       NOT NULL DEFAULT '{}'::jsonb,

    state            TEXT        NOT NULL
        CHECK (state IN ('PENDING', 'SENT', 'ACKED', 'REJECTED', 'TIMED_OUT')),

    -- MAVLink COMMAND_ACK carries a result code; kept as sent so an operator sees
    -- what the vehicle actually said rather than this platform's interpretation.
    ack_result       INTEGER,
    ack_at           TIMESTAMPTZ,

    issued_by        TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at       TIMESTAMPTZ NOT NULL,

    -- Scoped to the tenant, not global: two tenants must be able to use the same
    -- key without one seeing evidence of the other's traffic.
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX command_device_idx ON command (tenant_id, device_id, created_at DESC);

-- Finding commands that have outlived their window. Partial, because only the two
-- unresolved states can time out and they are a small minority of the table.
CREATE INDEX command_awaiting_ack_idx ON command (expires_at)
    WHERE state IN ('PENDING', 'SENT');

-- -------------------------------------------------------------------- outbox --

-- The transactional outbox.
--
-- A command cannot be written to this database and published to Kafka atomically:
-- there are two systems and no shared transaction. Publishing first loses the
-- record if the insert then fails -- a vehicle acting on a command nothing knows
-- about. Inserting first loses the publish if the broker is unreachable -- a
-- command an operator believes was sent.
--
-- So both writes go into one database transaction, and a relay reads this table
-- and publishes. The database is the source of truth; Kafka is a projection of it.
-- Delivery is at-least-once, which is why commands carry idempotency keys and why
-- ACK matching is by command id rather than by arrival order.
CREATE TABLE command_outbox (
    -- GENERATED ALWAYS rather than BIGSERIAL: the relay orders by this column, and
    -- an application that could supply its own value could insert one out of order.
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    command_id   UUID        NOT NULL REFERENCES command (id) ON DELETE CASCADE,
    topic        TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0
);

-- The relay's only query: unpublished rows, oldest first. Partial so the index
-- holds the backlog rather than the whole history -- in steady state it is empty,
-- and an index over every command ever sent would be scanned to find nothing.
CREATE INDEX command_outbox_unpublished_idx ON command_outbox (created_at)
    WHERE published_at IS NULL;

-- ----------------------------------------------------------------- audit log --

-- Append-only, enforced here rather than in application code.
--
-- An audit log that the application promises not to modify is worth exactly as
-- much as the application's next bug. Enforcing it in the database means a direct
-- psql session, a migration, an ORM flush and a compromised service account all
-- hit the same wall.
CREATE TABLE audit_log (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_id  TEXT        NOT NULL,
    actor      TEXT        NOT NULL,
    action     TEXT        NOT NULL,
    subject    TEXT,
    outcome    TEXT        NOT NULL CHECK (outcome IN ('ALLOWED', 'DENIED', 'FAILED')),
    detail     JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX audit_log_tenant_at_idx ON audit_log (tenant_id, at DESC);
CREATE INDEX audit_log_actor_at_idx  ON audit_log (actor, at DESC);

-- A denied request is the one an auditor comes looking for, and it is rare, so it
-- gets its own partial index rather than being found by scanning the rest.
CREATE INDEX audit_log_denied_idx ON audit_log (tenant_id, at DESC)
    WHERE outcome = 'DENIED';

CREATE OR REPLACE FUNCTION audit_log_is_append_only() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only: % is not permitted', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$ LANGUAGE plpgsql;

-- FOR EACH STATEMENT, not FOR EACH ROW: a DELETE matching nothing should still be
-- refused. A row-level trigger never fires on an empty match, so `DELETE FROM
-- audit_log WHERE false` would succeed silently and read, in a log of what the
-- database rejected, as though deletion were allowed.
CREATE TRIGGER audit_log_no_update
    BEFORE UPDATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit_log_is_append_only();

CREATE TRIGGER audit_log_no_delete
    BEFORE DELETE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit_log_is_append_only();

CREATE TRIGGER audit_log_no_truncate
    BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit_log_is_append_only();
