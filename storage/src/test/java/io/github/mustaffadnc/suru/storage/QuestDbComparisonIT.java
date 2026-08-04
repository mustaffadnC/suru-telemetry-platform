package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Measures QuestDB on the same data and shape, for ADR-0005.
 *
 * <p>ADR-0001 chose TimescaleDB provisionally and said the choice would be measured rather than
 * argued. This is that measurement. It is deliberately run against QuestDB's own designed ingest
 * path — the line protocol — rather than through its PostgreSQL wire compatibility, because
 * comparing a vendor's fast path against another vendor's slow one produces a number that says
 * nothing.
 *
 * <p>Gated behind {@code -Dsuru.dbbench=true} and run separately from the TimescaleDB measurement:
 * two databases saturating the same disk at once would measure the contention rather than either
 * engine.
 *
 * <p><b>The table is called {@code suru_telemetry}, not {@code telemetry}.</b> QuestDB keeps an
 * internal table of that exact name for its own telemetry, and the collision does not announce
 * itself: {@code CREATE TABLE} is accepted, {@code tables()} then lists nothing, and the line
 * protocol logs "could not get table writer" for every row while a handful leak through. A first
 * attempt landed 9 of 1000 rows and produced nearly three million error lines in the container log
 * — the run looked like it was ingesting slowly rather than failing. Renaming the table fixes it
 * completely: 1000 of 1000, {@code walEnabled=true}.
 *
 * <pre>
 * ./gradlew :storage:test --rerun -Dsuru.dbbench=true --tests '*QuestDbComparisonIT*'
 * </pre>
 */
@EnabledIfSystemProperty(named = "suru.dbbench", matches = "true")
class QuestDbComparisonIT {

    private static final int DEVICES = Integer.getInteger("suru.dbbench.devices", 50);
    private static final int METRICS = Integer.getInteger("suru.dbbench.metrics", 20);
    private static final long TARGET_ROWS = Long.getLong("suru.dbbench.rows", 100_000_000L);

    private static GenericContainer<?> questdb;
    private static String jdbcUrl;

    @BeforeAll
    static void startQuestDb() {
        questdb =
                // Server pinned to the same version as the client dependency: the line protocol
                // is versioned, and a mismatch is the kind of thing that fails at the first
                // flush rather than at startup.
                new GenericContainer<>("questdb/questdb:9.4.3")
                        .withExposedPorts(9000, 8812, 9009)
                        .withEnv("QDB_LINE_TCP_WRITER_WORKER_COUNT", "4")
                        // QuestDB's own telemetry writes to a table while we are measuring
                        // ingest; off, so the numbers describe our data and not its bookkeeping.
                        .withEnv("QDB_TELEMETRY_ENABLED", "false")
                        .waitingFor(Wait.forHttp("/").forPort(9000).forStatusCode(200));
        questdb.start();
        jdbcUrl =
                "jdbc:postgresql://%s:%d/qdb"
                        .formatted(questdb.getHost(), questdb.getMappedPort(8812));
    }

    @AfterAll
    static void stopQuestDb() {
        if (questdb != null) {
            questdb.stop();
        }
    }

    private static Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "quest");
        props.setProperty("sslmode", "disable");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    private static String queryOne(String sql) throws SQLException {
        try (Connection connection = connect();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static double[] timeQuery(String label, String sql, int runs) throws SQLException {
        queryOne(sql); // warm-up, discarded
        double[] millis = new double[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            queryOne(sql);
            millis[i] = (System.nanoTime() - start) / 1e6;
        }
        Arrays.sort(millis);
        double p50 = millis[runs / 2];
        double p95 = millis[(int) Math.min(runs - 1L, Math.round(runs * 0.95))];
        System.out.printf("[qdb-query] %-40s p50 %8.1f ms   p95 %8.1f ms%n", label, p50, p95);
        return new double[] {p50, p95};
    }

    @Test
    @DisplayName("Measurement: QuestDB ingest, footprint and query cost on identical data")
    void measureQuestDb() throws Exception {
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            // SYMBOL is QuestDB's interned string — the right type for low-cardinality
            // dimensions, and the fair counterpart to what TimescaleDB's segmentby exploits.
            // Using VARCHAR here would hand TimescaleDB an advantage QuestDB does not have to
            // concede.
            statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS suru_telemetry (
                      ts     TIMESTAMP,
                      tenant SYMBOL,
                      device SYMBOL,
                      metric SYMBOL,
                      value  DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL
                    """);
        }

        int series = DEVICES * METRICS;
        long secondsNeeded = TARGET_ROWS / series;
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        System.out.printf(
                "[qdb] %,d series, %,d seconds of history, target %,d rows%n",
                series, secondsNeeded, TARGET_ROWS);

        long written = 0;
        long ingestStart = System.nanoTime();

        // The line protocol written directly. There is no client dependency here on purpose:
        // org.questdb:questdb ships the server and contains no Sender, and
        // org.questdb:questdb-client is a stale 1.x artifact that stopped tracking the server
        // long ago. The protocol itself is plain text — measurement,tags fields timestamp — so
        // writing it is less code than resolving which library is current, and removes a
        // version-mismatch failure mode from a measurement whose whole point is trust.
        try (Socket socket = new Socket(questdb.getHost(), questdb.getMappedPort(9009));
                OutputStream raw = socket.getOutputStream();
                BufferedOutputStream out = new BufferedOutputStream(raw, 1 << 20)) {
            socket.setTcpNoDelay(false);
            StringBuilder line = new StringBuilder(160);

            for (long second = 0; second < secondsNeeded; second++) {
                long nanos = start.plusSeconds(second).getEpochSecond() * 1_000_000_000L;
                for (int d = 0; d < DEVICES; d++) {
                    String device = "link-%03d/sys1".formatted(d);
                    for (int m = 0; m < METRICS; m++) {
                        double value =
                                10.0 + m + (second % 3600) * 0.001 + ((second + d) % 17) * 0.01;
                        line.setLength(0);
                        line.append("suru_telemetry,tenant=bench,device=")
                                .append(device)
                                .append(",metric=bench.metric_")
                                .append(m < 10 ? "0" : "")
                                .append(m)
                                .append(" value=")
                                .append(value)
                                .append(' ')
                                .append(nanos)
                                .append('\n');
                        out.write(line.toString().getBytes(StandardCharsets.UTF_8));
                        written++;
                    }
                }
                if (second % 5000 == 0 && second > 0) {
                    out.flush();
                    double elapsed = (System.nanoTime() - ingestStart) / 1e9;
                    System.out.printf(
                            "[qdb] %,d rows in %.0f s (%,.0f rows/s)%n",
                            written, elapsed, written / elapsed);
                }
            }
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException("line protocol ingest failed", e);
        }

        // WAL tables apply asynchronously; the ingest is not finished until the row count
        // settles, and stopping the clock at flush would report a rate the database has not
        // actually achieved.
        long expected = written;
        long settleStart = System.nanoTime();
        long counted = 0;
        while (System.nanoTime() - settleStart < 600_000_000_000L) {
            counted = Long.parseLong(queryOne("SELECT count() FROM suru_telemetry"));
            if (counted >= expected) {
                break;
            }
            Thread.sleep(500);
        }
        double ingestSeconds = (System.nanoTime() - ingestStart) / 1e9;

        System.out.printf(
                "%n[qdb] INGEST %,d rows in %.1f s = %,.0f rows/s (incl. WAL apply)%n",
                counted, ingestSeconds, counted / ingestSeconds);

        String diskBytes = queryOne("SELECT sum(diskSize) FROM table_storage() WHERE tableName='suru_telemetry'");
        if (diskBytes != null) {
            long bytes = Long.parseLong(diskBytes);
            System.out.printf(
                    "[qdb] FOOTPRINT %,.2f GB (%.1f bytes/row)%n",
                    bytes / 1e9, (double) bytes / counted);
        }

        System.out.println("\n[qdb] --- queries ---");
        timeQuery(
                "1 device+metric, 1 hour",
                """
                SELECT count() FROM suru_telemetry
                WHERE tenant='bench' AND device='link-007/sys1' AND metric='bench.metric_03'
                  AND ts >= '2026-01-01T05:00:00.000000Z' AND ts < '2026-01-01T06:00:00.000000Z'
                """,
                20);

        timeQuery(
                "one series, 5-minute buckets, 24 hours",
                """
                SELECT count() FROM (
                  SELECT ts, avg(value) FROM suru_telemetry
                  WHERE tenant='bench' AND device='link-007/sys1' AND metric='bench.metric_03'
                    AND ts >= '2026-01-01T00:00:00.000000Z' AND ts < '2026-01-02T00:00:00.000000Z'
                  SAMPLE BY 5m)
                """,
                20);

        timeQuery(
                "one series, 6-hour buckets, whole range",
                """
                SELECT count() FROM (
                  SELECT ts, avg(value) FROM suru_telemetry
                  WHERE tenant='bench' AND device='link-007/sys1' AND metric='bench.metric_03'
                  SAMPLE BY 6h)
                """,
                10);

        timeQuery(
                "latest value per metric, one device",
                """
                SELECT count() FROM (
                  SELECT metric, last(value) FROM suru_telemetry
                  WHERE tenant='bench' AND device='link-007/sys1'
                  GROUP BY metric)
                """,
                20);

        timeQuery(
                "one metric across all devices",
                """
                SELECT count() FROM (
                  SELECT device, max(value) FROM suru_telemetry
                  WHERE tenant='bench' AND metric='bench.metric_03'
                  GROUP BY device)
                """,
                10);

        assertThat(counted).isGreaterThanOrEqualTo(expected - series);
    }
}
