-- Base schema: the relational side, and the telemetry hypertable.
--
-- Verified against TimescaleDB 2.29.0 on PostgreSQL 17.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------- relational --

CREATE TABLE tenant (
    tenant_id   TEXT PRIMARY KEY,
    display_name TEXT        NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE device (
    tenant_id   TEXT        NOT NULL REFERENCES tenant (tenant_id) ON DELETE CASCADE,
    device_id   TEXT        NOT NULL,
    -- The link a device was first seen on, and its MAVLink system id. Both are
    -- observed rather than configured: a vehicle announces itself by transmitting.
    link_id     TEXT,
    system_id   INTEGER,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, device_id)
);

CREATE INDEX device_last_seen_idx ON device (tenant_id, last_seen DESC);

-- ---------------------------------------------------------------- telemetry --

-- A narrow measurement table rather than a column per field.
--
-- MAVLink defines hundreds of messages with wholly different shapes, so a wide
-- table would need either one table per message type — unmanageable — or a column
-- per field across all of them, most of them null in most rows. The narrow form
-- costs more rows but every row is the same shape, which is exactly what the
-- columnar compression below is built to exploit: within one (device, metric)
-- segment, time is monotonic and value is a slowly varying double, so both
-- compress to a fraction of their raw size.
CREATE TABLE telemetry (
    time      TIMESTAMPTZ      NOT NULL,
    tenant_id TEXT             NOT NULL,
    device_id TEXT             NOT NULL,
    metric    TEXT             NOT NULL,
    value     DOUBLE PRECISION NOT NULL
);

-- One-hour chunks. Telemetry arrives fast, and the chunk interval decides how much
-- data the write path keeps hot: the active chunk and its indexes want to sit in
-- memory. An hour of a busy fleet is a few hundred megabytes here, and shorter
-- chunks also mean compression starts paying off within the hour rather than the day.
SELECT create_hypertable('telemetry', by_range('time', INTERVAL '1 hour'));

-- Queries are always "this device's this metric over this window", never a scan
-- across devices, so the composite index leads with the equality columns and ends
-- with time descending — newest first, which is the direction dashboards read.
CREATE INDEX telemetry_device_metric_time_idx
    ON telemetry (tenant_id, device_id, metric, time DESC);

-- A second index for the cross-device case: "every vehicle's battery right now".
CREATE INDEX telemetry_metric_time_idx
    ON telemetry (tenant_id, metric, time DESC);
