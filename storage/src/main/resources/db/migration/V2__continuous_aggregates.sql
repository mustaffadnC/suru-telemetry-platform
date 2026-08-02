-- Rollups.
--
-- A dashboard asking for a month of battery voltage does not want two million raw
-- samples; it wants a few hundred points. Without pre-aggregation the database
-- reads and discards the difference on every request. Continuous aggregates keep
-- the rollups materialised and incrementally refreshed, so the cost is paid once
-- at write time instead of on every read.
--
-- Runs outside a transaction: CREATE MATERIALIZED VIEW ... WITH
-- (timescaledb.continuous) cannot execute inside one.

CREATE MATERIALIZED VIEW telemetry_1m
    WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       tenant_id,
       device_id,
       metric,
       avg(value)   AS avg_value,
       min(value)   AS min_value,
       max(value)   AS max_value,
       count(*)     AS samples
FROM telemetry
GROUP BY bucket, tenant_id, device_id, metric
WITH NO DATA;

-- Built on the minute rollup, not on the raw table. A hierarchical aggregate reads
-- 60 pre-computed rows per hour instead of re-reading every raw sample, so the
-- refresh cost stays flat as retention grows.
CREATE MATERIALIZED VIEW telemetry_1h
    WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 hour', bucket) AS bucket,
       tenant_id,
       device_id,
       metric,
       avg(avg_value) AS avg_value,
       min(min_value) AS min_value,
       max(max_value) AS max_value,
       sum(samples)   AS samples
FROM telemetry_1m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

-- NOTE on avg-of-avg: averaging the minute averages is only exact when every
-- minute holds the same number of samples, which telemetry does not guarantee.
-- The sample counts are carried through so a consumer needing an exact mean can
-- compute sum(avg_value * samples) / sum(samples); min and max are exact either
-- way. The approximation is accepted here because the hourly view exists for
-- trend display, and the exact path remains available.
