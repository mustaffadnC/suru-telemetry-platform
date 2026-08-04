-- Composite indexes on the continuous aggregates.
--
-- The raw table has had one since V1. The aggregates did not, and the omission is easy to make:
-- they are addressed as views, so they look like they inherit the table's access paths. They do
-- not — each is backed by its own hypertable with its own indexes.
--
-- TimescaleDB does create indexes on a continuous aggregate automatically, but one per GROUP BY
-- column: (tenant_id, bucket), (device_id, bucket), (metric, bucket). A query filtering on all
-- three can only use one of them and then filters the rest, so its cost tracks how much data the
-- rollup holds in total rather than how much the caller asked for.
--
-- Measured on a ten-million-row load, one series over the full range:
--   without this index   p95  8.0 ms
--   with this index      p95  1.7 ms   (4.8x)
--
-- The symptom at a hundred million rows was worse and stranger: the minute rollup came out slower
-- than scanning the raw table, which is the opposite of the reason rollups exist. See
-- docs/benchmarks.md.

CREATE INDEX IF NOT EXISTS telemetry_1m_series_idx
    ON telemetry_1m (tenant_id, device_id, metric, bucket DESC);

CREATE INDEX IF NOT EXISTS telemetry_1h_series_idx
    ON telemetry_1h (tenant_id, device_id, metric, bucket DESC);
