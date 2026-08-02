-- Compression, refresh and retention.
--
-- Runs outside a transaction: the policy functions schedule background jobs and
-- several of them refuse to run inside one.

-- ------------------------------------------------------------- compression --

-- Columnstore, segmented by the columns every query filters on. Segmenting is the
-- decision that makes or breaks the compression ratio: within one
-- (tenant, device, metric) segment the rows are a monotonic timestamp and a
-- slowly-varying double, which delta and Gorilla encoding reduce to a few bits
-- each. Segmenting by something that changes every row would compress almost
-- nothing, and segmenting by too many columns makes the segments too small to
-- amortise their headers.
--
-- 'timescaledb.enable_columnstore' is the 2.18+ spelling; earlier releases wrote
-- the same thing as 'timescaledb.compress' with compress_segmentby/compress_orderby.
ALTER TABLE telemetry SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tenant_id, device_id, metric',
    timescaledb.orderby = 'time DESC'
    );

-- Compress anything older than two hours. Long enough that the chunk being written
-- to is never touched, short enough that most of the dataset is compressed most of
-- the time.
--
-- CALL, not SELECT: add_columnstore_policy is a procedure, unlike the retention and
-- continuous-aggregate policies below, which are functions. Calling it with SELECT
-- fails with "add_columnstore_policy(...) is a procedure".
CALL add_columnstore_policy('telemetry', after => INTERVAL '2 hours');

-- ------------------------------------------------------------- cagg refresh --

-- start_offset bounds how far back a refresh looks; end_offset keeps the refresh
-- away from the most recent minute, which is still being written to and would be
-- materialised incomplete.
SELECT add_continuous_aggregate_policy('telemetry_1m',
                                       start_offset => INTERVAL '3 hours',
                                       end_offset => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('telemetry_1h',
                                       start_offset => INTERVAL '3 days',
                                       end_offset => INTERVAL '1 hour',
                                       schedule_interval => INTERVAL '30 minutes');

-- -------------------------------------------------------------- retention --

-- Tiered deliberately. Raw samples answer "what exactly happened during this
-- flight" and stop being asked for once the flight is old; the rollups answer
-- "how has this airframe behaved over months" and are three orders of magnitude
-- smaller. Keeping raw data forever would mean paying for the former to serve the
-- latter.
SELECT add_retention_policy('telemetry', drop_after => INTERVAL '7 days');
SELECT add_retention_policy('telemetry_1m', drop_after => INTERVAL '90 days');
-- telemetry_1h has no retention policy: it is the long-term record.
