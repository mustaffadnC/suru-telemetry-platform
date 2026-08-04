package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies the schema is what the migrations claim, against a real TimescaleDB.
 *
 * <p>Asserting that Flyway reported success proves only that the SQL ran. What matters is whether
 * the table is actually a hypertable, whether compression is actually segmented by the columns the
 * queries filter on, and whether the policies are actually scheduled — all of which can be silently
 * absent while every statement succeeds.
 */
class TelemetrySchemaIT {

    private static TimescaleTestDatabase db;

    @BeforeAll
    static void startDatabase() {
        db = TimescaleTestDatabase.startAndMigrate();
    }

    @AfterAll
    static void stopDatabase() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    @DisplayName("telemetry is a hypertable with one-hour chunks, not an ordinary table")
    void telemetryIsAHypertable() throws SQLException {
        String hypertable =
                db.queryOne(
                        "SELECT hypertable_name FROM timescaledb_information.hypertables"
                                + " WHERE hypertable_name = 'telemetry'");
        assertThat(hypertable).isEqualTo("telemetry");

        // A wrong chunk interval does not fail anything — it just quietly makes the write path
        // hold far more or far less data hot than intended.
        String intervalMicros =
                db.queryOne(
                        "SELECT d.interval_length FROM _timescaledb_catalog.dimension d"
                                + " JOIN _timescaledb_catalog.hypertable h ON h.id = d.hypertable_id"
                                + " WHERE h.table_name = 'telemetry'");
        assertThat(Long.parseLong(intervalMicros))
                .as("chunk interval in microseconds")
                .isEqualTo(3_600_000_000L);
    }

    @Test
    @DisplayName("Compression is segmented by the columns queries filter on")
    void compressionIsSegmentedCorrectly() throws SQLException {
        List<String> segmentBy =
                db.queryColumn(
                        "SELECT attname FROM timescaledb_information.compression_settings"
                                + " WHERE hypertable_name = 'telemetry' AND segmentby_column_index IS NOT NULL"
                                + " ORDER BY segmentby_column_index");

        // Segmenting is the decision the compression ratio hinges on: inside one segment the
        // timestamp is monotonic and the value slowly varying, which is what delta and Gorilla
        // encoding exploit. Segment by something that changes every row and the ratio collapses.
        assertThat(segmentBy).containsExactly("tenant_id", "device_id", "metric");

        List<String> orderBy =
                db.queryColumn(
                        "SELECT attname FROM timescaledb_information.compression_settings"
                                + " WHERE hypertable_name = 'telemetry' AND orderby_column_index IS NOT NULL"
                                + " ORDER BY orderby_column_index");
        assertThat(orderBy).containsExactly("time");
    }

    @Test
    @DisplayName("Both rollups exist and the hourly one is built on the minute one")
    void continuousAggregatesAreHierarchical() throws SQLException {
        List<String> views =
                db.queryColumn(
                        "SELECT view_name FROM timescaledb_information.continuous_aggregates"
                                + " ORDER BY view_name");
        assertThat(views).containsExactly("telemetry_1h", "telemetry_1m");

        // The hourly view must read the minute rollup, not the raw table: 60 pre-computed rows
        // per hour instead of every raw sample, so refresh cost stays flat as retention grows.
        // Timescale records a continuous aggregate's source in hypertable_name — for a
        // hierarchical one that is the parent's materialisation hypertable, not 'telemetry'.
        String minuteSource =
                db.queryOne(
                        "SELECT hypertable_name FROM timescaledb_information.continuous_aggregates"
                                + " WHERE view_name = 'telemetry_1m'");
        assertThat(minuteSource).isEqualTo("telemetry");

        String hourlySource =
                db.queryOne(
                        "SELECT hypertable_name FROM timescaledb_information.continuous_aggregates"
                                + " WHERE view_name = 'telemetry_1h'");
        assertThat(hourlySource)
                .as("the hourly rollup must be built on the minute rollup, not on raw telemetry")
                .isNotEqualTo("telemetry")
                .startsWith("_materialized_hypertable_");
    }

    @Test
    @DisplayName("Compression, refresh and retention are actually scheduled")
    void policiesAreScheduled() throws SQLException {
        List<String> jobs =
                db.queryColumn(
                        "SELECT proc_name || ':' || hypertable_name"
                                + " FROM timescaledb_information.jobs"
                                + " WHERE hypertable_name IS NOT NULL"
                                + " ORDER BY 1");

        // A policy that was never created leaves a schema that looks right and grows without
        // bound — the sort of thing noticed in production rather than in review.
        //
        // Note the procedure names: the columnstore policy is still recorded as
        // 'policy_compression'. The 2.18+ columnstore API is a renaming over the same
        // machinery, and only the job's application_name reflects the new wording.
        assertThat(jobs)
                .contains(
                        "policy_compression:telemetry",
                        "policy_refresh_continuous_aggregate:telemetry_1m",
                        "policy_refresh_continuous_aggregate:telemetry_1h",
                        "policy_retention:telemetry",
                        "policy_retention:telemetry_1m");

        // telemetry_1h deliberately has none: it is the long-term record.
        assertThat(jobs).doesNotContain("policy_retention:telemetry_1h");
    }

    @Test
    @DisplayName("The composite index leading with the equality columns exists")
    void queryIndexExists() throws SQLException {
        List<String> indexes =
                db.queryColumn(
                        "SELECT indexname FROM pg_indexes WHERE tablename = 'telemetry' ORDER BY 1");
        assertThat(indexes)
                .contains("telemetry_device_metric_time_idx", "telemetry_metric_time_idx");
    }

    @Test
    @DisplayName("The rollups carry the composite index too, not just the raw table")
    void rollupsHaveTheirOwnCompositeIndex() throws SQLException {
        // TimescaleDB indexes a continuous aggregate automatically, but one index per GROUP BY
        // column. A query filtering tenant, device and metric together can use only one of
        // those and filters the rest, so its cost tracks total rollup size rather than the
        // series asked for — measured at 4.8x on a ten-million-row load.
        List<String> rollupIndexes =
                db.queryColumn(
                        "SELECT indexname FROM pg_indexes"
                                + " WHERE indexname IN ('telemetry_1m_series_idx',"
                                + " 'telemetry_1h_series_idx') ORDER BY 1");
        assertThat(rollupIndexes)
                .containsExactly("telemetry_1h_series_idx", "telemetry_1m_series_idx");
    }

    @Test
    @DisplayName("Running the migrations again is a no-op")
    void migrationsAreIdempotent() {
        // Flyway records what it has applied; re-running must not attempt the hypertable or the
        // policies a second time, both of which would fail.
        int applied = TelemetrySchema.migrate(db.dataSource());
        assertThat(applied).isZero();
    }
}
