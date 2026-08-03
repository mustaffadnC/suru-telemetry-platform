package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.ingest.netty.TelemetryGateway;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The whole path, end to end: a socket, a gateway, Kafka, a decoder, and TimescaleDB.
 *
 * <p>Every layer has its own tests. This one exists because the interesting failures live between
 * them — a header the writer does not read, a timestamp that survives one hop and not the next, a
 * message that decodes in isolation and produces nothing once it has been through a topic.
 */
class TelemetryPipelineIT {

    private static KafkaContainer kafka;
    private static TimescaleTestDatabase db;
    private static byte[] sitlStream;

    @BeforeAll
    static void startInfrastructure() throws Exception {
        try (InputStream in =
                TelemetryPipelineIT.class.getResourceAsStream("/mavlink/sitl_stream.bin")) {
            if (in == null) {
                throw new IllegalStateException("sitl_stream.bin missing from the test classpath");
            }
            sitlStream = in.readAllBytes();
        }
        kafka = new KafkaContainer("apache/kafka:latest");
        kafka.start();
        db = TimescaleTestDatabase.startAndMigrate();
    }

    /**
     * A topic of its own for each test.
     *
     * <p>Sharing one would make the tests interfere in a way that looks like a product bug:
     * truncating the table between tests does not unpublish anything, so a consumer reading from
     * the earliest offset sees every previous test's records too. The first version of this class
     * shared a topic and reported 3174 records consumed where 1058 were expected — exactly three
     * runs' worth.
     */
    private static String freshTopic() throws Exception {
        String topic = "telemetry.raw." + System.nanoTime();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get();
        }
        return topic;
    }

    @AfterAll
    static void stopInfrastructure() {
        if (db != null) {
            db.close();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    private static void sendOverTcp(InetSocketAddress address, byte[] data) {
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setTcpNoDelay(true);
            OutputStream out = socket.getOutputStream();
            for (int off = 0; off < data.length; off += 1024) {
                out.write(data, off, Math.min(1024, data.length - off));
            }
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Streams the recording through a gateway into Kafka, and waits for the broker to have it. */
    private static void publishRecordedFlight(String topic) throws Exception {
        AdmissionController admission = new AdmissionController();
        try (KafkaTelemetryPublisher publisher =
                        new KafkaTelemetryPublisher(kafka.getBootstrapServers(), topic);
                TelemetryGateway gateway =
                        new TelemetryGateway(
                                MavlinkDialect.arduPilotMega(),
                                admission,
                                publisher,
                                DeviceRegistry.open())) {
            InetSocketAddress address = gateway.start(0);
            sendOverTcp(address, sitlStream);

            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (admission.stats().accepted() < 1058 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            while (admission.stats().inFlight() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            publisher.flush();
        }
    }

    private static TelemetryIngestService serviceFor(
            String topic, String groupId, TelemetryIngestService.BatchWriter writer) {
        KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(
                        TelemetryIngestService.defaultProperties(
                                kafka.getBootstrapServers(), groupId));
        consumer.subscribe(List.of(topic));
        return new TelemetryIngestService(consumer, writer, Duration.ofSeconds(2));
    }

    /** Drains the topic until two consecutive polls produce nothing. */
    private static long drain(TelemetryIngestService service) throws SQLException {
        long total = 0;
        int emptyPolls = 0;
        while (emptyPolls < 3) {
            long written = service.pollOnce();
            total += written;
            emptyPolls = written == 0 ? emptyPolls + 1 : 0;
        }
        return total;
    }

    @Test
    @DisplayName("A flight streamed into a socket ends up queryable in TimescaleDB")
    void endToEnd() throws Exception {
        db.execute("TRUNCATE telemetry");
        String topic = freshTopic();
        publishRecordedFlight(topic);

        TelemetryIngestService service =
                serviceFor(
                        topic,
                        "pipeline-" + System.nanoTime(),
                        new TelemetryCopyWriter(db.dataSource())::write);
        long written;
        try (service) {
            written = drain(service);
        }

        assertThat(written).isPositive();
        assertThat(db.queryOne("SELECT count(*) FROM telemetry")).isEqualTo(String.valueOf(written));

        // The position the aircraft actually reached, having travelled through a socket, a
        // decoder, a Kafka topic, a second decoder and a COPY. Any layer mangling the payload
        // or the offsets shows up here as a coordinate somewhere else on Earth.
        assertThat(
                        db.queryOne(
                                "SELECT round(max(value)::numeric, 6) FROM telemetry"
                                        + " WHERE metric = 'position.latitude_deg'"))
                .isEqualTo("39.925533");
        assertThat(
                        db.queryOne(
                                "SELECT round(max(value)::numeric, 6) FROM telemetry"
                                        + " WHERE metric = 'position.longitude_deg'"))
                .isEqualTo("32.866287");
        assertThat(
                        db.queryOne(
                                "SELECT round(max(value)::numeric, 1) FROM telemetry"
                                        + " WHERE metric = 'power.battery_v'"))
                .isEqualTo("12.6");

        // Timestamps must be wall clock. Before the receivedAt field was fixed these landed
        // decades away, and nothing upstream noticed because nothing upstream stored them.
        assertThat(db.queryOne("SELECT count(*) FROM telemetry WHERE time > now() - interval '1 hour'"))
                .isEqualTo(String.valueOf(written));

        // Only the curated message set contributes rows; the rest passed through leaving
        // nothing, which is the intended behaviour rather than a gap.
        assertThat(service.stats().recordsWithoutMetrics()).isPositive();
        assertThat(service.stats().recordsConsumed()).isEqualTo(1058);
    }

    @Test
    @DisplayName("A failed write leaves the batch on the topic instead of losing it")
    void failedWriteIsRedelivered() throws Exception {
        db.execute("TRUNCATE telemetry");
        String topic = freshTopic();
        publishRecordedFlight(topic);

        String group = "redelivery-" + System.nanoTime();

        // The write fails. Offsets must not move: committing before the database would make this
        // batch vanish silently, because Kafka would consider it delivered and nothing would ever
        // ask for it again.
        try (TelemetryIngestService failing =
                serviceFor(
                        topic,
                        group,
                        rows -> {
                            throw new SQLException("induced failure");
                        })) {
            assertThatThrownBy(failing::pollOnce)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("induced");
        }

        // A fresh consumer in the same group must see the records again.
        long written;
        TelemetryIngestService recovering =
                serviceFor(topic, group, new TelemetryCopyWriter(db.dataSource())::write);
        try (recovering) {
            written = drain(recovering);
        }

        assertThat(written).isPositive();
        assertThat(db.queryOne("SELECT count(*) FROM telemetry")).isEqualTo(String.valueOf(written));
    }

    @Test
    @DisplayName("Rows aggregate into the minute rollup once it is refreshed")
    void rollupMaterialises() throws Exception {
        db.execute("TRUNCATE telemetry");
        String topic = freshTopic();
        publishRecordedFlight(topic);

        try (TelemetryIngestService service =
                serviceFor(
                        topic,
                        "rollup-" + System.nanoTime(),
                        new TelemetryCopyWriter(db.dataSource())::write)) {
            drain(service);
        }

        // Continuous aggregates refresh on a schedule; a test cannot wait for it, so the
        // refresh is invoked directly. What is being checked is that the rollup's grouping
        // matches the raw data, not that the scheduler runs.
        db.execute("CALL refresh_continuous_aggregate('telemetry_1m', NULL, NULL)");

        String rawBattery =
                db.queryOne(
                        "SELECT round(max(value)::numeric, 3) FROM telemetry"
                                + " WHERE metric = 'power.battery_v'");
        String rolledBattery =
                db.queryOne(
                        "SELECT round(max(max_value)::numeric, 3) FROM telemetry_1m"
                                + " WHERE metric = 'power.battery_v'");

        assertThat(rolledBattery).isEqualTo(rawBattery);
        assertThat(Long.parseLong(db.queryOne("SELECT coalesce(sum(samples), 0) FROM telemetry_1m")))
                .isEqualTo(Long.parseLong(db.queryOne("SELECT count(*) FROM telemetry")));
    }
}
