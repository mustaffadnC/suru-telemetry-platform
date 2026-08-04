package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
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
 * Finds out why the minute rollup measured slower than the raw table.
 *
 * <p>The hundred-million-row run put the one-minute rollup at 70.9 ms p95 — an order of magnitude
 * worse than scanning raw over the same series, and worse than the hourly rollup over a range
 * twenty-eight times wider. Rollups exist to be cheaper than raw. That one was not.
 *
 * <p>The first hypothesis was real-time aggregation, and it was wrong: {@code materialized_only}
 * turns out to default to <em>true</em> on TimescaleDB 2.29, so that union never existed, and
 * toggling it changes nothing. The same query at ten million rows costs 8.6 ms rather than 70.9 —
 * roughly one eighth, for roughly one eighth of the buckets. Cost tracking total data rather than
 * the requested series is what a missing index looks like.
 *
 * <p>The raw hypertable carries a composite index on {@code (tenant_id, device_id, metric, time)}.
 * The continuous aggregates were created without one, which is easy to miss because they are
 * <em>views</em> and look like they inherit the table's access paths. They do not: each is backed
 * by its own hypertable with its own indexes.
 *
 * <pre>
 * ./gradlew :storage:test --rerun -Dsuru.dbbench=true --tests '*RollupOverheadIT*'
 * </pre>
 */
@EnabledIfSystemProperty(named = "suru.dbbench", matches = "true")
class RollupOverheadIT {

    private static final int DEVICES = 20;
    private static final int METRICS = 10;
    private static final long ROWS = Long.getLong("suru.rollup.rows", 10_000_000L);

    private static TimescaleTestDatabase db;

    @BeforeAll
    static void loadData() throws SQLException {
        db = TimescaleTestDatabase.startAndMigrate();
        TelemetryCopyWriter writer = new TelemetryCopyWriter(db.dataSource());

        int series = DEVICES * METRICS;
        long seconds = ROWS / series;
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        List<TelemetryRow> batch = new ArrayList<>(500_000);

        for (long s = 0; s < seconds; s++) {
            Instant at = start.plusSeconds(s);
            for (int d = 0; d < DEVICES; d++) {
                for (int m = 0; m < METRICS; m++) {
                    batch.add(
                            new TelemetryRow(
                                    at,
                                    "bench",
                                    "link-%03d/sys1".formatted(d),
                                    "bench.metric_%02d".formatted(m),
                                    10.0 + m + (s % 3600) * 0.001));
                }
            }
            if (batch.size() >= 500_000) {
                writer.write(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            writer.write(batch);
        }

        db.execute("SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('telemetry') c");
        db.execute("CALL refresh_continuous_aggregate('telemetry_1m', NULL, NULL)");
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.close();
        }
    }

    private static double[] time(String label, String sql, int runs) throws SQLException {
        db.queryOne(sql); // warm-up, discarded
        double[] millis = new double[runs];
        for (int i = 0; i < runs; i++) {
            long t = System.nanoTime();
            db.queryOne(sql);
            millis[i] = (System.nanoTime() - t) / 1e6;
        }
        Arrays.sort(millis);
        double p50 = millis[runs / 2];
        double p95 = millis[(int) Math.min(runs - 1L, Math.round(runs * 0.95))];
        System.out.printf("[rollup] %-46s p50 %8.1f ms   p95 %8.1f ms%n", label, p50, p95);
        return new double[] {p50, p95};
    }

    private static final String QUERY =
            """
            SELECT count(*) FROM (
              SELECT time_bucket(INTERVAL '5 minutes', bucket) b,
                     sum(avg_value*samples)/sum(samples)
              FROM telemetry_1m
              WHERE tenant_id='bench' AND device_id='link-007/sys1'
                AND metric='bench.metric_03'
              GROUP BY b) s
            """;

    @Test
    @DisplayName("Measurement: does the rollup lack the index the raw table has")
    void rollupNeedsItsOwnIndex() throws SQLException {
        System.out.printf("[rollup] %,d rows loaded, rollup refreshed%n", ROWS);

        // Recorded rather than assumed: the first version of this test asserted the default was
        // 'f' and failed, which is how the real-time aggregation hypothesis died.
        String materializedOnly =
                db.queryOne(
                        "SELECT materialized_only FROM timescaledb_information.continuous_aggregates"
                                + " WHERE view_name='telemetry_1m'");
        System.out.printf(
                "[rollup] materialized_only = %s (so real-time aggregation is not in play)%n",
                materializedOnly);

        List<String> before = indexesOnRollup();
        System.out.printf("[rollup] indexes on the rollup before: %s%n", before);

        double[] withoutIndex = time("rollup query, no composite index", QUERY, 20);

        // The raw table has this; the aggregates were created without it. They are views, which
        // makes it easy to assume they inherit the table's access paths — each is in fact backed
        // by its own hypertable with its own indexes.
        db.execute(
                "CREATE INDEX telemetry_1m_series_idx ON telemetry_1m"
                        + " (tenant_id, device_id, metric, bucket DESC)");
        db.execute("ANALYZE telemetry_1m");

        double[] withIndex = time("rollup query, composite index added", QUERY, 20);

        double speedup = withoutIndex[1] / withIndex[1];
        System.out.printf(
                "%n[rollup] adding the composite index: p95 %.1f ms -> %.1f ms (%.1fx)%n",
                withoutIndex[1], withIndex[1], speedup);

        // The index must not change the answer, only the path to it.
        assertThat(db.queryOne(QUERY)).isNotNull();
        assertThat(indexesOnRollup()).contains("telemetry_1m_series_idx");
    }

    private static List<String> indexesOnRollup() throws SQLException {
        return db.queryColumn(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = (SELECT materialization_hypertable_schema
                                    FROM timescaledb_information.continuous_aggregates
                                    WHERE view_name='telemetry_1m')
                   OR indexname LIKE 'telemetry_1m%'
                ORDER BY 1
                """);
    }
}
