package io.github.mustaffadnc.suru.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.Geofence;
import io.github.mustaffadnc.suru.rules.Rule;
import io.github.mustaffadnc.suru.rules.RuleEngine;
import io.github.mustaffadnc.suru.rules.Severity;
import io.github.mustaffadnc.suru.rules.Staleness;
import io.github.mustaffadnc.suru.rules.Threshold;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The three phase-4 scenarios driven through the real topology.
 *
 * <p>{@link TopologyTestDriver} rather than a broker, because the telemetry-loss scenario is
 * governed by wall-clock punctuation and the driver is the only thing that lets a test move the
 * wall clock. Against a real broker the same test would have to sleep for the staleness timeout,
 * which makes it slow, and then flake anyway on a loaded CI runner.
 */
class AlertTopologyTest {

    private static final String TELEMETRY_TOPIC = "telemetry.raw";
    private static final String ALERT_TOPIC = "alerts";
    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";

    private static final double CENTRE_LAT = 39.8917;
    private static final double CENTRE_LON = 32.7833;
    private static final double METRES_PER_DEGREE_LAT = 111_194.93;

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> telemetry;
    private TestOutputTopic<String, Alert> alerts;

    private static RuleEngine fleetRules() {
        return new RuleEngine(
                List.of(
                        new Rule(
                                "geofence",
                                "Left the operating area",
                                TENANT,
                                Rule.ALL_DEVICES,
                                Geofence.around(CENTRE_LAT, CENTRE_LON, 1000),
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(10),
                                Severity.CRITICAL),
                        new Rule(
                                "battery",
                                "Battery low",
                                TENANT,
                                Rule.ALL_DEVICES,
                                new Threshold(
                                        "power.battery_remaining_pct",
                                        Threshold.Comparison.BELOW,
                                        20.0,
                                        25.0),
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(10),
                                Severity.WARNING),
                        new Rule(
                                "silence",
                                "Telemetry lost",
                                TENANT,
                                Rule.ALL_DEVICES,
                                Staleness.after(Duration.ofSeconds(15)),
                                Duration.ZERO,
                                Duration.ZERO,
                                Severity.CRITICAL)));
    }

    @BeforeEach
    void startDriver() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suru-alerts-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");

        AlertTopology.Config config =
                new AlertTopology.Config(
                        TELEMETRY_TOPIC,
                        ALERT_TOPIC,
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(5),
                         64,
                         8,
                        false);

        driver = new TopologyTestDriver(AlertTopology.build(fleetRules(), config), props, T0);
        telemetry =
                driver.createInputTopic(
                        TELEMETRY_TOPIC, new StringSerializer(), new ByteArraySerializer());
        alerts =
                driver.createOutputTopic(
                        ALERT_TOPIC,
                        new org.apache.kafka.common.serialization.StringDeserializer(),
                        JsonSerde.of(Alert.class).deserializer());
    }

    @AfterEach
    void stopDriver() {
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * Advances the clock one punctuation interval at a time, as production does.
     *
     * <p>{@code advanceWallClockTime} fires each wall-clock punctuator once per call however far it
     * moves the clock — twenty seconds against a one-second schedule is one tick, not twenty. A
     * single large jump would test a cadence the deployed processor never runs at.
     */
    private void tick(int seconds) {
        for (int i = 0; i < seconds; i++) {
            driver.advanceWallClockTime(Duration.ofSeconds(1));
        }
    }

    @Test
    @DisplayName("scenario: a vehicle leaves the operating area and an alert reaches the topic")
    void geofenceBreachReachesTheAlertTopic() {
        sendPosition(200, T0);
        sendPosition(200, T0.plusSeconds(1));
        assertThat(alerts.isEmpty()).isTrue();

        sendPosition(1500, T0.plusSeconds(2));
        assertThat(alerts.isEmpty()).as("held by the five-second debounce").isTrue();

        sendPosition(1500, T0.plusSeconds(7));
        List<Alert> fired = alerts.readValuesToList();

        assertThat(fired).hasSize(1);
        assertThat(fired.getFirst().ruleId()).isEqualTo("geofence");
        assertThat(fired.getFirst().kind()).isEqualTo(Alert.Kind.FIRED);
        assertThat(fired.getFirst().deviceId()).isEqualTo(DEVICE);
        assertThat(fired.getFirst().conditionSince()).isEqualTo(T0.plusSeconds(2));
    }

    @Test
    @DisplayName("scenario: the battery drains and the alert survives a round trip through JSON")
    void batteryAlertSerialisesAndDeserialises() {
        sendBattery(18, T0);
        sendBattery(17, T0.plusSeconds(5));
        assertThat(alerts.isEmpty()).isTrue();

        sendBattery(16, T0.plusSeconds(10));
        Alert alert = alerts.readValuesToList().getFirst();

        assertThat(alert.ruleId()).isEqualTo("battery");
        assertThat(alert.severity()).isEqualTo(Severity.WARNING);
        assertThat(alert.conditionSince())
                .as("nanosecond-precision instants have to survive the serde")
                .isEqualTo(T0);
        assertThat(alert.debounce()).isEqualTo(Duration.ofSeconds(10));
        assertThat(alert.detail()).contains("power.battery_remaining_pct");
    }

    @Test
    @DisplayName("scenario: a vehicle stops transmitting and the timer notices")
    void telemetryLossFiresFromPunctuation() {
        sendPosition(100, T0);
        assertThat(alerts.isEmpty()).isTrue();

        // No records at all from here. Only the wall clock moves.
        tick(10);
        assertThat(alerts.isEmpty()).as("under the fifteen-second limit").isTrue();

        tick(10);
        List<Alert> fired = alerts.readValuesToList();

        assertThat(fired)
                .as("no record arrived to trigger this — it can only have come from the timer")
                .hasSize(1);
        assertThat(fired.getFirst().ruleId()).isEqualTo("silence");
        assertThat(fired.getFirst().severity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    @DisplayName("telemetry loss resolves when the device comes back")
    void telemetryLossResolves() {
        sendPosition(100, T0);
        tick(20);
        assertThat(alerts.readValuesToList()).hasSize(1);

        sendPosition(100, T0.plusSeconds(20));
        List<Alert> resolved = alerts.readValuesToList();

        assertThat(resolved).hasSize(1);
        assertThat(resolved.getFirst().kind()).isEqualTo(Alert.Kind.RESOLVED);
    }

    @Test
    @DisplayName("a heartbeat carries no metrics and still proves the device is alive")
    void heartbeatSuppressesTelemetryLoss() {
        sendPosition(100, T0);

        // Heartbeats only, at 5 s intervals. None of them produces a stored measurement.
        for (int t = 5; t <= 30; t += 5) {
            send(MavlinkMetrics.MSG_HEARTBEAT, new byte[9], T0.plusSeconds(t));
            tick(5);
        }

        assertThat(alerts.readValuesToList())
                .as("treating 'produced no metrics' as 'did not arrive' would fire here")
                .isEmpty();
    }

    @Test
    @DisplayName("a device only alerts under its own tenant's rules")
    void otherTenantsAreNotEvaluated() {
        send(
                MavlinkMetrics.MSG_GLOBAL_POSITION_INT,
                globalPositionPayload(0.0, 0.0),
                T0,
                "other-tenant",
                DEVICE);
        tick(60);

        assertThat(alerts.readValuesToList())
                .as("0N 0E is thousands of km outside the fence, and silent for a minute")
                .isEmpty();
    }

    @Test
    @DisplayName("a record without routing headers is dropped rather than crashing the processor")
    void unattributableRecordIsDropped() {
        telemetry.pipeInput(
                new org.apache.kafka.streams.test.TestRecord<>(
                        TENANT + '/' + DEVICE,
                        globalPositionPayload(CENTRE_LAT, CENTRE_LON),
                        new RecordHeaders(),
                        T0));

        assertThat(alerts.isEmpty()).isTrue();

        // And the processor is still working afterwards.
        sendPosition(1500, T0.plusSeconds(1));
        sendPosition(1500, T0.plusSeconds(7));
        assertThat(alerts.readValuesToList()).hasSize(1);
    }

    // --- input helpers -------------------------------------------------------------------

    private void sendPosition(double metresNorth, Instant at) {
        send(
                MavlinkMetrics.MSG_GLOBAL_POSITION_INT,
                globalPositionPayload(
                        CENTRE_LAT + metresNorth / METRES_PER_DEGREE_LAT, CENTRE_LON),
                at);
    }

    private void sendBattery(int remainingPercent, Instant at) {
        send(MavlinkMetrics.MSG_SYS_STATUS, sysStatusPayload(remainingPercent), at);
    }

    private void send(int messageId, byte[] payload, Instant at) {
        send(messageId, payload, at, TENANT, DEVICE);
    }

    private void send(int messageId, byte[] payload, Instant at, String tenant, String device) {
        long nanos = at.getEpochSecond() * 1_000_000_000L + at.getNano();
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_TENANT, bytes(tenant)));
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_DEVICE, bytes(device)));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_MESSAGE_ID,
                        bytes(Integer.toString(messageId))));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_RECEIVED_AT, bytes(Long.toString(nanos))));

        telemetry.pipeInput(
                new org.apache.kafka.streams.test.TestRecord<>(
                        tenant + '/' + device, payload, headers, at));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** GLOBAL_POSITION_INT: uint32 time, int32 lat, int32 lon, then the rest zeroed. */
    private static byte[] globalPositionPayload(double latitudeDeg, double longitudeDeg) {
        ByteBuffer buffer = ByteBuffer.allocate(28).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0, 0);
        buffer.putInt(4, (int) Math.round(latitudeDeg * 1e7));
        buffer.putInt(8, (int) Math.round(longitudeDeg * 1e7));
        buffer.putShort(26, (short) 65535); // heading unknown
        return buffer.array();
    }

    /** SYS_STATUS: battery_remaining is an int8 at offset 30. */
    private static byte[] sysStatusPayload(int remainingPercent) {
        ByteBuffer buffer = ByteBuffer.allocate(31).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(14, (short) 12_400); // 12.4 V
        buffer.putShort(16, (short) 500); // 5 A
        buffer.put(30, (byte) remainingPercent);
        return buffer.array();
    }
}
