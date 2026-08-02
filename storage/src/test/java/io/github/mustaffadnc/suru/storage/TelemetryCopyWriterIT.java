package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TelemetryCopyWriterIT {

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

    @BeforeEach
    void truncate() throws SQLException {
        db.execute("TRUNCATE telemetry");
    }

    private static List<TelemetryRow> rows(int count, String device, Instant start) {
        List<TelemetryRow> generated = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            generated.add(
                    new TelemetryRow(
                            start.plusMillis(i * 100L),
                            "tenant-a",
                            device,
                            "power.battery_v",
                            12.6 - (i % 100) * 0.001));
        }
        return generated;
    }

    @Test
    @DisplayName("Rows written by COPY come back with their values intact")
    void copyRoundTrips() throws SQLException {
        Instant start = Instant.parse("2026-08-01T10:00:00Z");
        List<TelemetryRow> batch = rows(1_000, "link-1/sys1", start);

        long written = writer.write(batch);

        assertThat(written).isEqualTo(1_000);
        assertThat(db.queryOne("SELECT count(*) FROM telemetry")).isEqualTo("1000");
        assertThat(
                        db.queryOne(
                                "SELECT value FROM telemetry WHERE time = '2026-08-01T10:00:00Z'"
                                        + " AND metric = 'power.battery_v'"))
                .isEqualTo("12.6");
        assertThat(db.queryOne("SELECT device_id FROM telemetry LIMIT 1")).isEqualTo("link-1/sys1");
    }

    @Test
    @DisplayName("Timestamps survive the text format without drifting")
    void timestampsRoundTrip() throws SQLException {
        Instant exact = Instant.parse("2026-08-01T10:00:00.123456Z");
        writer.write(List.of(new TelemetryRow(exact, "tenant-a", "dev", "m", 1.0)));

        // Microsecond precision is what PostgreSQL stores; anything finer is truncated rather
        // than rejected, so a drift here would be silent.
        assertThat(db.queryOne("SELECT time AT TIME ZONE 'UTC' FROM telemetry"))
                .isEqualTo("2026-08-01 10:00:00.123456");
    }

    @Test
    @DisplayName("A device id containing tabs or newlines cannot shift the columns")
    void escapesStructuralCharacters() throws SQLException {
        // Device ids derive from what a peer transmitted. An unescaped tab would move every
        // following field one column left — the value ending up in the metric name, and the
        // row either failing loudly or, worse, landing wrong.
        String hostile = "link\t1\nsys\\1";
        writer.write(List.of(new TelemetryRow(Instant.parse("2026-08-01T10:00:00Z"),
                "tenant-a", hostile, "power.battery_v", 12.6)));

        assertThat(db.queryOne("SELECT device_id FROM telemetry")).isEqualTo(hostile);
        assertThat(db.queryOne("SELECT metric FROM telemetry")).isEqualTo("power.battery_v");
        assertThat(db.queryOne("SELECT value FROM telemetry")).isEqualTo("12.6");
    }

    @Test
    @DisplayName("An empty batch is not a database round trip")
    void emptyBatchDoesNothing() throws SQLException {
        assertThat(writer.write(List.of())).isZero();
        assertThat(db.queryOne("SELECT count(*) FROM telemetry")).isEqualTo("0");
    }

    @Test
    @DisplayName("Replaying an identical batch leaves every aggregate but the count unchanged")
    void duplicateRowsAreHarmlessToAggregates() throws SQLException {
        // Delivery is at-least-once: the database commits before Kafka offsets do, so a crash
        // between them replays a batch. The replayed rows are byte-identical, and this is the
        // assertion that makes tolerating them defensible rather than merely convenient.
        Instant start = Instant.parse("2026-08-01T10:00:00Z");
        List<TelemetryRow> batch = rows(500, "link-1/sys1", start);

        writer.write(batch);
        String avgOnce = db.queryOne("SELECT round(avg(value)::numeric, 9) FROM telemetry");
        String minOnce = db.queryOne("SELECT min(value) FROM telemetry");
        String maxOnce = db.queryOne("SELECT max(value) FROM telemetry");

        writer.write(batch);

        assertThat(db.queryOne("SELECT count(*) FROM telemetry")).isEqualTo("1000");
        assertThat(db.queryOne("SELECT round(avg(value)::numeric, 9) FROM telemetry"))
                .isEqualTo(avgOnce);
        assertThat(db.queryOne("SELECT min(value) FROM telemetry")).isEqualTo(minOnce);
        assertThat(db.queryOne("SELECT max(value) FROM telemetry")).isEqualTo(maxOnce);
    }

    @Test
    @DisplayName("COPY is substantially faster than batched INSERT on the same rows")
    void copyOutperformsInsert() throws SQLException {
        int count = 50_000;
        Instant start = Instant.parse("2026-08-01T10:00:00Z");
        List<TelemetryRow> batch = rows(count, "link-bench/sys1", start);

        // Warm both paths so the comparison is not measuring first-call effects.
        writer.write(rows(1_000, "warm", start));
        writer.writeWithInsert(rows(1_000, "warm", start));
        db.execute("TRUNCATE telemetry");

        long insertNanos = System.nanoTime();
        writer.writeWithInsert(batch);
        insertNanos = System.nanoTime() - insertNanos;
        db.execute("TRUNCATE telemetry");

        long copyNanos = System.nanoTime();
        writer.write(batch);
        copyNanos = System.nanoTime() - copyNanos;

        double speedup = (double) insertNanos / copyNanos;
        System.out.printf(
                "[copy-vs-insert] %,d rows — INSERT %.0f ms (%,.0f rows/s), COPY %.0f ms (%,.0f rows/s), %.1fx%n",
                count,
                insertNanos / 1e6,
                count / (insertNanos / 1e9),
                copyNanos / 1e6,
                count / (copyNanos / 1e9),
                speedup);

        // Deliberately a loose bound. The exact ratio depends on the machine and on how loaded
        // it is; what the schema needs to guarantee is that the bulk path is bulk, and a
        // regression to parity would mean COPY had stopped being used at all.
        assertThat(speedup).isGreaterThan(1.5);
        assertThat(db.queryOne("SELECT count(*) FROM telemetry")).isEqualTo(String.valueOf(count));
    }

    @Test
    @DisplayName("Rows land in the hourly chunks their timestamps belong to")
    void rowsAreChunkedByHour() throws SQLException {
        Instant start = Instant.parse("2026-08-01T10:00:00Z").truncatedTo(ChronoUnit.HOURS);
        List<TelemetryRow> spanning = new ArrayList<>();
        for (int hour = 0; hour < 4; hour++) {
            spanning.add(
                    new TelemetryRow(
                            start.plus(Duration.ofHours(hour)), "tenant-a", "dev", "m", hour));
        }
        writer.write(spanning);

        // Four hours of data across one-hour chunks must produce four chunks; a wrong interval
        // would still store everything and only show up as a performance problem later.
        assertThat(
                        db.queryOne(
                                "SELECT count(*) FROM timescaledb_information.chunks"
                                        + " WHERE hypertable_name = 'telemetry'"))
                .isEqualTo("4");
    }
}
