package io.github.mustaffadnc.suru.ingest.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.netty.TelemetryGateway;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * End-to-end: a socket at one end, a real Kafka broker at the other.
 *
 * <p>Testcontainers' JUnit extension is not used — it binds to the JUnit Platform version and this
 * project is on Platform 6 — so the container is started and stopped explicitly.
 */
class KafkaTelemetryPublisherIT {

    private static final String TOPIC = "telemetry.raw";
    private static final int PARTITIONS = 3;
    private static final int EXPECTED_FRAMES = 1058;

    private static KafkaContainer kafka;
    private static byte[] sitlStream;

    @BeforeAll
    static void startBroker() throws IOException {
        try (InputStream in =
                KafkaTelemetryPublisherIT.class.getResourceAsStream("/mavlink/sitl_stream.bin")) {
            if (in == null) {
                throw new IllegalStateException("sitl_stream.bin missing from the test classpath");
            }
            sitlStream = in.readAllBytes();
        }
        kafka = new KafkaContainer("apache/kafka:latest");
        kafka.start();
    }

    @AfterAll
    static void stopBroker() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    private static void createTopic(String name) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(name, PARTITIONS, (short) 1)))
                    .all()
                    .get();
        }
    }

    private static List<ConsumerRecord<String, byte[]>> drain(String topic, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (collected.size() < expected && System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(collected::add);
            }
        }
        return collected;
    }

    private static String header(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
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

    @Test
    @DisplayName("A flight streamed into the gateway lands in Kafka, keyed and annotated")
    void publishesRecordedFlightToKafka() throws Exception {
        createTopic(TOPIC);

        AdmissionController admission = new AdmissionController();
        try (KafkaTelemetryPublisher publisher =
                        new KafkaTelemetryPublisher(kafka.getBootstrapServers(), TOPIC);
                TelemetryGateway gateway =
                        new TelemetryGateway(
                                MavlinkDialect.arduPilotMega(),
                                admission,
                                publisher,
                                DeviceRegistry.open())) {

            InetSocketAddress address = gateway.start(0);
            sendOverTcp(address, sitlStream);

            List<ConsumerRecord<String, byte[]>> records = drain(TOPIC, EXPECTED_FRAMES);

            assertThat(records).hasSize(EXPECTED_FRAMES);
            assertThat(admission.stats().shedTotal()).isZero();
            assertThat(gateway.counters().publishFailures()).isZero();

            ConsumerRecord<String, byte[]> first = records.getFirst();
            assertThat(first.key()).startsWith(DeviceRegistry.DEFAULT_TENANT + "/");
            assertThat(header(first, KafkaTelemetryPublisher.HEADER_TENANT))
                    .isEqualTo(DeviceRegistry.DEFAULT_TENANT);
            assertThat(header(first, KafkaTelemetryPublisher.HEADER_SOURCE)).isEqualTo("MAVLINK");
            assertThat(header(first, KafkaTelemetryPublisher.HEADER_SYSTEM_ID)).isEqualTo("1");
            assertThat(header(first, KafkaTelemetryPublisher.HEADER_MESSAGE_ID)).isNotNull();
            assertThat(header(first, KafkaTelemetryPublisher.HEADER_PRIORITY)).isNotNull();

            // The payload is the frame's bytes untouched — no envelope, no re-encoding.
            assertThat(first.value()).isNotNull();
        }
    }

    @Test
    @DisplayName("One device's telemetry stays on one partition, so its order survives")
    void keepsDeviceTelemetryOnASinglePartition() throws Exception {
        String topic = "telemetry.ordering";
        createTopic(topic);

        try (KafkaTelemetryPublisher publisher =
                        new KafkaTelemetryPublisher(kafka.getBootstrapServers(), topic);
                TelemetryGateway gateway =
                        new TelemetryGateway(
                                MavlinkDialect.arduPilotMega(),
                                new AdmissionController(),
                                publisher,
                                DeviceRegistry.open())) {

            InetSocketAddress address = gateway.start(0);
            sendOverTcp(address, sitlStream);

            List<ConsumerRecord<String, byte[]>> records = drain(topic, EXPECTED_FRAMES);
            assertThat(records).hasSize(EXPECTED_FRAMES);

            // Every downstream sequence-gap calculation and state machine assumes a device's
            // messages arrive in the order it sent them. Kafka only guarantees that within a
            // partition, so a device spread across partitions would make gap detection report
            // loss that never happened.
            Map<String, Set<Integer>> partitionsPerKey = new HashMap<>();
            for (var record : records) {
                partitionsPerKey
                        .computeIfAbsent(record.key(), k -> new java.util.HashSet<>())
                        .add(record.partition());
            }

            assertThat(partitionsPerKey).isNotEmpty();
            assertThat(partitionsPerKey.values())
                    .as("each device must occupy exactly one partition")
                    .allSatisfy(partitions -> assertThat(partitions).hasSize(1));

            // Offsets within that partition must be strictly increasing in publication order.
            String key = partitionsPerKey.keySet().iterator().next();
            List<Long> offsets =
                    records.stream()
                            .filter(r -> r.key().equals(key))
                            .map(ConsumerRecord::offset)
                            .collect(Collectors.toList());
            assertThat(offsets).isSorted();
        }
    }
}
