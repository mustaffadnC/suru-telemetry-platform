package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Loads the store at scale and measures what it costs.
 *
 * <p>Gated behind {@code -Dsuru.dbbench=true} because it writes a hundred million rows and takes
 * tens of minutes; it is a measurement, not a regression test, and its numbers go into
 * {@code docs/benchmarks.md}.
 *
 * <pre>
 * ./gradlew :storage:test --rerun -Dsuru.dbbench=true --tests '*ScaleMeasurementIT*'
 * </pre>
 */
@EnabledIfSystemProperty(named = "suru.dbbench", matches = "true")
class ScaleMeasurementIT {

    /** Series count: devices times metrics. Each series produces one sample per second. */
    private static final int DEVICES = Integer.getInteger("suru.dbbench.devices", 50);

    private static final int METRICS = Integer.getInteger("suru.dbbench.metrics", 20);

    private static final long TARGET_ROWS = Long.getLong("suru.dbbench.rows", 100_000_000L);

    /** Rows per COPY. Large enough to amortise the round trip, small enough to stay in memory. */
    private static final int BATCH = 500_000;

    private static TimescaleTestDatabase db;
    private static TelemetryCopyWriter writer;

    @BeforeAll
    static void startDatabase() {
        db = TimescaleTestDatabase.startAndMigrate();
        writer = new TelemetryCopyWriter(db.dataSource());
    }

    @AfterAll
    static void stopDatabase() {
        if (db != null) {
            db.close();
        }
    }

    private static String metricName(int index) {
        return "bench.metric_%02d".formatted(index);
    }

    private static String deviceName(int index) {
        return "link-%03d/sys1".formatted(index);
    }

    /** Times a query over several runs and returns its percentiles in milliseconds. */
    private static double[] timeQuery(String label, String sql, int runs) throws SQLException {
        // One warm-up pass, discarded: the first execution pays for planning and for pulling
        // pages that every later one finds already cached, and reporting it would describe a
        // cold start rather than the steady state a dashboard actually sees.
        db.queryOne(sql);

        double[] millis = new double[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            db.queryOne(sql);
            millis[i] = (System.nanoTime() - start) / 1e6;
        }
        Arrays.sort(millis);
        double p50 = millis[runs / 2];
        double p95 = millis[(int) Math.min(runs - 1L, Math.round(runs * 0.95))];
        System.out.printf("[query] %-42s p50 %8.1f ms   p95 %8.1f ms%n", label, p50, p95);
        return new double[] {p50, p95};
    }

    private static long sizeBytes(String relation) throws SQLException {
        return Long.parseLong(db.queryOne("SELECT hypertable_size('" + relation + "')"));
    }

    @Test
    @DisplayName("Measurement: ingest, compression and query cost at a hundred million rows")
    void measureAtScale() throws SQLException {
        int series = DEVICES * METRICS;
        long secondsNeeded = TARGET_ROWS / series;
        Instant start = Instant.parse("2026-01-01T00:00:00Z");

        System.out.printf(
                "[scale] %,d series (%d devices x %d metrics), %,d seconds of history, target %,d rows%n",
                series, DEVICES, METRICS, secondsNeeded, TARGET_ROWS);

        long written = 0;
        long ingestStart = System.nanoTime();
        List<TelemetryRow> batch = new ArrayList<>(BATCH);

        for (long second = 0; second < secondsNeeded; second++) {
            Instant at = start.plusSeconds(second);
            for (int d = 0; d < DEVICES; d++) {
                String device = deviceName(d);
                for (int m = 0; m < METRICS; m++) {
                    // A slow drift with a per-series offset and a ripple: constant values would
                    // compress far better than real telemetry and flatter the ratio.
                    double value =
                            10.0 + m + (second % 3600) * 0.001 + ((second + d) % 17) * 0.01;
                    batch.add(new TelemetryRow(at, "bench", device, metricName(m), value));
                }
            }
            if (batch.size() >= BATCH) {
                written += writer.write(batch);
                batch.clear();
                if (written % (BATCH * 20L) == 0) {
                    double elapsed = (System.nanoTime() - ingestStart) / 1e9;
                    System.out.printf(
                            "[scale] %,d rows in %.0f s (%,.0f rows/s)%n",
                            written, elapsed, written / elapsed);
                }
            }
        }
        if (!batch.isEmpty()) {
            written += writer.write(batch);
        }
        double ingestSeconds = (System.nanoTime() - ingestStart) / 1e9;

        System.out.printf(
                "%n[scale] INGEST %,d rows in %.1f s = %,.0f rows/s%n",
                written, ingestSeconds, written / ingestSeconds);

        long uncompressed = sizeBytes("telemetry");
        String chunks = db.queryOne(
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry'");
        System.out.printf(
                "[scale] UNCOMPRESSED %,.2f GB across %s chunks (%.1f bytes/row)%n",
                uncompressed / 1e9, chunks, (double) uncompressed / written);

        // Queries before compression, so the comparison afterwards is like for like.
        System.out.println("\n[scale] --- queries on uncompressed data ---");
        double[] rawBefore =
                timeQuery(
                        "raw: 1 device+metric, 1 hour",
                        """
                        SELECT count(*) FROM telemetry
                        WHERE tenant_id='bench' AND device_id='%s' AND metric='%s'
                          AND time >= TIMESTAMPTZ '2026-01-01 05:00:00Z'
                          AND time <  TIMESTAMPTZ '2026-01-01 06:00:00Z'
                        """
                                .formatted(deviceName(7), metricName(3)),
                        20);

        System.out.println("\n[scale] compressing every chunk...");
        long compressStart = System.nanoTime();
        db.execute("SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('telemetry') c");
        double compressSeconds = (System.nanoTime() - compressStart) / 1e9;

        long compressed = sizeBytes("telemetry");
        System.out.printf(
                "[scale] COMPRESSED %,.2f GB in %.0f s — ratio %.1fx, %.1f bytes/row%n",
                compressed / 1e9,
                compressSeconds,
                (double) uncompressed / compressed,
                (double) compressed / written);

        // Refresh the rollups so the resolution-selecting queries have something to read.
        System.out.println("\n[scale] refreshing rollups...");
        long refreshStart = System.nanoTime();
        db.execute("CALL refresh_continuous_aggregate('telemetry_1m', NULL, NULL)");
        db.execute("CALL refresh_continuous_aggregate('telemetry_1h', NULL, NULL)");
        System.out.printf(
                "[scale] rollups refreshed in %.0f s%n", (System.nanoTime() - refreshStart) / 1e9);

        System.out.println("\n[scale] --- queries on compressed data ---");
        double[] rawAfter =
                timeQuery(
                        "raw: 1 device+metric, 1 hour",
                        """
                        SELECT count(*) FROM telemetry
                        WHERE tenant_id='bench' AND device_id='%s' AND metric='%s'
                          AND time >= TIMESTAMPTZ '2026-01-01 05:00:00Z'
                          AND time <  TIMESTAMPTZ '2026-01-01 06:00:00Z'
                        """
                                .formatted(deviceName(7), metricName(3)),
                        20);

        timeQuery(
                "raw: whole range, one series, bucketed",
                """
                SELECT count(*) FROM (
                  SELECT time_bucket(INTERVAL '5 minutes', time) b, avg(value)
                  FROM telemetry
                  WHERE tenant_id='bench' AND device_id='%s' AND metric='%s'
                  GROUP BY b) s
                """
                        .formatted(deviceName(7), metricName(3)),
                5);

        timeQuery(
                "1m rollup: one series, 24 hours",
                """
                SELECT count(*) FROM (
                  SELECT time_bucket(INTERVAL '5 minutes', bucket) b,
                         sum(avg_value*samples)/sum(samples)
                  FROM telemetry_1m
                  WHERE tenant_id='bench' AND device_id='%s' AND metric='%s'
                    AND bucket >= TIMESTAMPTZ '2026-01-01 00:00:00Z'
                    AND bucket <  TIMESTAMPTZ '2026-01-02 00:00:00Z'
                  GROUP BY b) s
                """
                        .formatted(deviceName(7), metricName(3)),
                20);

        timeQuery(
                "1h rollup: one series, whole range",
                """
                SELECT count(*) FROM (
                  SELECT time_bucket(INTERVAL '6 hours', bucket) b,
                         sum(avg_value*samples)/sum(samples)
                  FROM telemetry_1h
                  WHERE tenant_id='bench' AND device_id='%s' AND metric='%s'
                  GROUP BY b) s
                """
                        .formatted(deviceName(7), metricName(3)),
                20);

        timeQuery(
                "latest value per metric, one device",
                """
                SELECT count(*) FROM (
                  SELECT DISTINCT ON (metric) metric, value
                  FROM telemetry
                  WHERE tenant_id='bench' AND device_id='%s'
                    AND time >= TIMESTAMPTZ '2026-01-01 20:00:00Z'
                  ORDER BY metric, time DESC) s
                """
                        .formatted(deviceName(7)),
                20);

        timeQuery(
                "one metric across all devices, 1h rollup",
                """
                SELECT count(*) FROM (
                  SELECT device_id, max(max_value)
                  FROM telemetry_1h
                  WHERE tenant_id='bench' AND metric='%s'
                  GROUP BY device_id) s
                """
                        .formatted(metricName(3)),
                20);

        // The materialisation hypertable lives in _timescaledb_internal, so the schema has to be
        // part of the identifier — a bare name will not resolve to regclass.
        long rollupSize =
                Long.parseLong(
                        db.queryOne(
                                "SELECT hypertable_size(format('%I.%I',"
                                    + " materialization_hypertable_schema,"
                                    + " materialization_hypertable_name)::regclass)"
                                    + " FROM timescaledb_information.continuous_aggregates"
                                    + " WHERE view_name='telemetry_1m'"));
        System.out.printf(
                "%n[scale] 1m rollup occupies %,.2f GB (%.1f%% of compressed raw)%n",
                rollupSize / 1e9, 100.0 * rollupSize / compressed);

        assertThat(written).isGreaterThanOrEqualTo(TARGET_ROWS - series);
        assertThat(compressed).isLessThan(uncompressed);
        // Reading a compressed chunk should not be catastrophically worse than reading a
        // native one; a large regression here would mean the segmenting is wrong.
        assertThat(rawAfter[1]).isLessThan(rawBefore[1] * 20);
    }
}
