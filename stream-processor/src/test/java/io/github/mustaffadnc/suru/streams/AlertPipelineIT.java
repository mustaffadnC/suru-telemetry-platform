package io.github.mustaffadnc.suru.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.Rule;
import io.github.mustaffadnc.suru.rules.RuleEngine;
import io.github.mustaffadnc.suru.rules.Severity;
import io.github.mustaffadnc.suru.rules.Threshold;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The alert path through a real broker and a real {@link KafkaStreams} instance.
 *
 * <p>{@code AlertTopologyTest} proves the topology's logic with the test driver; this proves the
 * thing actually runs — serdes resolved, stores created, changelog topics provisioned, the sink
 * writing something a separate consumer can read back. Those are failures a test driver cannot
 * have, because it never talks to a broker.
 *
 * <p>The latency measurement is gated behind {@code -Dsuru.alertbench=true} so an ordinary
 * {@code check} does not pay for hundreds of round trips:
 *
 * <pre>
 * ./gradlew :stream-processor:test --rerun -Dsuru.alertbench=true --tests '*AlertPipelineIT*'
 * </pre>
 */
class AlertPipelineIT {

    private static final String TENANT = "acme";
    private static final int LATENCY_SAMPLES = Integer.getInteger("suru.alertbench.samples", 200);
    private static final int WARMUP_SAMPLES = 20;

    private static final Deserializer<Alert> ALERT_DESERIALIZER =
            JsonSerde.of(Alert.class).deserializer();

    private static KafkaContainer kafka;

    @BeforeAll
    static void startKafka() {
        kafka = new KafkaContainer("apache/kafka:latest");
        kafka.start();
    }

    @AfterAll
    static void stopKafka() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    /** Battery below 20 %, no debounce: the first breaching record produces an alert. */
    private static RuleEngine immediateBatteryRule() {
        return new RuleEngine(
                List.of(
                        new Rule(
                                "battery",
                                "Battery low",
                                TENANT,
                                Rule.ALL_DEVICES,
                                Threshold.at(
                                        "power.battery_remaining_pct",
                                        Threshold.Comparison.BELOW,
                                        20.0),
                                Duration.ZERO,
                                Duration.ZERO,
                                Severity.CRITICAL)));
    }

    @Test
    @DisplayName("an alert travels from telemetry topic to alert topic through a real broker")
    void alertReachesTheTopic() throws Exception {
        Topics topics = freshTopics();
        KafkaStreams streams = startStreams(topics);
        try (Producer<String, byte[]> producer = producer();
                Consumer<String, byte[]> consumer = alertConsumer(topics)) {

            publishBattery(producer, topics, "link/sys1", 10, Instant.now());
            producer.flush();

            Alert alert = awaitOneAlert(consumer, Duration.ofSeconds(60));

            assertThat(alert.ruleId()).isEqualTo("battery");
            assertThat(alert.deviceId()).isEqualTo("link/sys1");
            assertThat(alert.kind()).isEqualTo(Alert.Kind.FIRED);
            assertThat(alert.severity()).isEqualTo(Severity.CRITICAL);
            assertThat(alert.detail()).contains("power.battery_remaining_pct");
        } finally {
            streams.close(Duration.ofSeconds(20));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "suru.alertbench", matches = "true")
    @DisplayName("Measurement: telemetry event to alert delivery")
    void measureAlertLatency() throws Exception {
        Topics topics = freshTopics();
        KafkaStreams streams = startStreams(topics);
        try (Producer<String, byte[]> producer = producer();
                Consumer<String, byte[]> consumer = alertConsumer(topics)) {

            // The first alerts pay for JIT, metadata fetches and store initialisation. Reporting
            // them would measure start-up rather than steady state.
            runRound(producer, consumer, topics, "warmup", WARMUP_SAMPLES);

            List<Long> latencies = runRound(producer, consumer, topics, "dev", LATENCY_SAMPLES);
            latencies.sort(Long::compareTo);

            System.out.printf(
                    "[alert] %d events, one device each, threshold rule with zero debounce%n",
                    latencies.size());
            System.out.printf(
                    "[alert] publish to alert readable   p50 %6.1f ms   p95 %6.1f ms"
                            + "   p99 %6.1f ms   max %6.1f ms%n",
                    millis(percentile(latencies, 50)),
                    millis(percentile(latencies, 95)),
                    millis(percentile(latencies, 99)),
                    millis(latencies.getLast()));

            assertThat(latencies).hasSize(LATENCY_SAMPLES);
        } finally {
            streams.close(Duration.ofSeconds(20));
        }
    }

    /**
     * Publishes one breaching record, waits for its alert, and repeats.
     *
     * <p><b>One at a time, deliberately.</b> The first version of this published all the events in
     * a tight loop and then drained the alert topic, which produced p50 154 ms and p95, p99 and max
     * all within 0.3 ms of 327 ms. That shape is not a latency distribution — it is a queue
     * emptying at a constant rate. Every event's clock had started at roughly the same instant, so
     * what was being measured was how long the burst took to drain, with the last event charged for
     * all 199 ahead of it. It would have been reported as latency and it would have been wrong by
     * an order of magnitude.
     *
     * <p>A sequential round trip measures what the number is supposed to mean: how long after a
     * vehicle reports a fault an operator can see the alert, on an otherwise idle pipeline.
     * Behaviour under load is a throughput question and belongs to phase 6.
     *
     * <p>Every device is distinct because a second breach from a device already firing is correctly
     * silent — that is what the state machine is for — and reusing one would wait forever.
     */
    private List<Long> runRound(
            Producer<String, byte[]> producer,
            Consumer<String, byte[]> consumer,
            Topics topics,
            String prefix,
            int count) {
        List<Long> latencies = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String device = prefix + "/sys" + i;
            long sent = System.nanoTime();
            publishBattery(producer, topics, device, 10, Instant.now());
            producer.flush();

            long deadline = sent + Duration.ofSeconds(30).toNanos();
            boolean seen = false;
            while (!seen && System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(5));
                long received = System.nanoTime();
                for (ConsumerRecord<String, byte[]> record : records) {
                    Alert alert = ALERT_DESERIALIZER.deserialize(record.topic(), record.value());
                    if (device.equals(alert.deviceId())) {
                        latencies.add(received - sent);
                        seen = true;
                    }
                }
            }
            if (!seen) {
                throw new AssertionError("no alert for " + device + " within the deadline");
            }
        }
        return latencies;
    }

    private Alert awaitOneAlert(Consumer<String, byte[]> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, byte[]> record : records) {
                return ALERT_DESERIALIZER.deserialize(record.topic(), record.value());
            }
        }
        throw new AssertionError("no alert arrived within " + timeout);
    }

    // --- infrastructure ------------------------------------------------------------------

    private record Topics(String telemetry, String alerts, String applicationId) {}

    private static Topics freshTopics() throws Exception {
        long stamp = System.nanoTime();
        Topics topics =
                new Topics("telemetry.raw." + stamp, "alerts." + stamp, "suru-alerts-" + stamp);
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(
                            List.of(
                                    new NewTopic(topics.telemetry(), 3, (short) 1),
                                    new NewTopic(topics.alerts(), 3, (short) 1)))
                    .all()
                    .get();
        }
        return topics;
    }

    private static KafkaStreams startStreams(Topics topics) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, topics.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        // One broker cannot satisfy the default replication factor for the changelog topics.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("java.io.tmpdir") + "/suru-streams-" + System.nanoTime());
        // Latency rather than throughput: the default 100 ms commit interval would land whole in
        // the measurement, and the point is to measure the path, not the batching policy.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);

        // Kafka Streams overrides the plain producer default of linger.ms=0 with 100, trading
        // latency for batching. Left at the default it dominates this measurement completely.
        // Settable so the comparison can be run rather than asserted.
        String linger = System.getProperty("suru.alertbench.linger");
        if (linger != null) {
            props.put(
                    StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG),
                    Integer.parseInt(linger));
        }

        AlertTopology.Config config =
                new AlertTopology.Config(
                        topics.telemetry(),
                        topics.alerts(),
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(1),
                        false);

        KafkaStreams streams =
                new KafkaStreams(AlertTopology.build(immediateBatteryRule(), config), props);

        CountDownLatch running = new CountDownLatch(1);
        streams.setStateListener(
                (newState, oldState) -> {
                    if (newState == KafkaStreams.State.RUNNING) {
                        running.countDown();
                    }
                });
        streams.start();
        if (!running.await(90, TimeUnit.SECONDS)) {
            streams.close(Duration.ofSeconds(10));
            throw new IllegalStateException("streams did not reach RUNNING: " + streams.state());
        }
        return streams;
    }

    private static Producer<String, byte[]> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private static Consumer<String, byte[]> alertConsumer(Topics topics) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-reader-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        // Poll for latency, not for batch size.
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10);

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topics.alerts()));
        return consumer;
    }

    private static void publishBattery(
            Producer<String, byte[]> producer,
            Topics topics,
            String deviceId,
            int percent,
            Instant at) {
        long nanos = at.getEpochSecond() * 1_000_000_000L + at.getNano();
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(
                        topics.telemetry(),
                        null,
                        at.toEpochMilli(),
                        TENANT + '/' + deviceId,
                        sysStatusPayload(percent));
        record.headers().add(KafkaTelemetryPublisher.HEADER_TENANT, bytes(TENANT));
        record.headers().add(KafkaTelemetryPublisher.HEADER_DEVICE, bytes(deviceId));
        record.headers()
                .add(
                        KafkaTelemetryPublisher.HEADER_MESSAGE_ID,
                        bytes(Integer.toString(MavlinkMetrics.MSG_SYS_STATUS)));
        record.headers()
                .add(KafkaTelemetryPublisher.HEADER_RECEIVED_AT, bytes(Long.toString(nanos)));
        producer.send(record);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** SYS_STATUS: battery_remaining is an int8 at offset 30. */
    private static byte[] sysStatusPayload(int remainingPercent) {
        ByteBuffer buffer = ByteBuffer.allocate(31).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(14, (short) 12_400);
        buffer.putShort(16, (short) 500);
        buffer.put(30, (byte) remainingPercent);
        return buffer.array();
    }

    private static long percentile(List<Long> sorted, int percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.clamp(index, 0, sorted.size() - 1));
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }
}
