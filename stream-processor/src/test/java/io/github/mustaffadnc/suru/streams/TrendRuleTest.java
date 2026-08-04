package io.github.mustaffadnc.suru.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.DerivedMetrics;
import io.github.mustaffadnc.suru.rules.Rule;
import io.github.mustaffadnc.suru.rules.RuleEngine;
import io.github.mustaffadnc.suru.rules.Severity;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Trend rules, expressed as thresholds on derived metric names.
 *
 * <p>The rate of change is the alert that actually helps: a battery at 40 % is fine, and a battery
 * at 40 % falling 8 % a minute means the vehicle has about five minutes. A level threshold cannot
 * distinguish them, and by the time it fires the margin is gone.
 */
class TrendRuleTest {

    private static final String TELEMETRY_TOPIC = "telemetry.raw";
    private static final String ALERT_TOPIC = "alerts";
    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";
    private static final String BATTERY = "power.battery_remaining_pct";

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> telemetry;
    private TestOutputTopic<String, Alert> alerts;

    /** Fires when the battery is dropping faster than 5 % a minute, clearing above 3 %. */
    private static RuleEngine drainRateRule() {
        return new RuleEngine(
                List.of(
                        new Rule(
                                "drain-rate",
                                "Battery draining fast",
                                TENANT,
                                Rule.ALL_DEVICES,
                                new Threshold(
                                        DerivedMetrics.name(BATTERY, DerivedMetrics.SLOPE_PER_MIN),
                                        Threshold.Comparison.BELOW,
                                        -5.0,
                                        -3.0),
                                Duration.ZERO,
                                Duration.ZERO,
                                Severity.WARNING)));
    }

    @BeforeEach
    void startDriver() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suru-trend-test");
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

        driver = new TopologyTestDriver(AlertTopology.build(drainRateRule(), config), props, T0);
        telemetry =
                driver.createInputTopic(
                        TELEMETRY_TOPIC, new StringSerializer(), new ByteArraySerializer());
        alerts =
                driver.createOutputTopic(
                        ALERT_TOPIC,
                        new StringDeserializer(),
                        JsonSerde.of(Alert.class).deserializer());
    }

    @AfterEach
    void stopDriver() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    @DisplayName("a fast drain fires even while the level is comfortable")
    void fastDrainFires() {
        // 8 %/min, sampled every 15 s. The level never drops below 76 %, so no threshold rule
        // anywhere in the fleet would have anything to say about this vehicle.
        for (int i = 0; i < 8; i++) {
            sendBattery(90 - 2 * i, T0.plusSeconds(i * 15L));
        }

        List<Alert> fired = alerts.readValuesToList();
        assertThat(fired).hasSize(1);
        assertThat(fired.getFirst().ruleId()).isEqualTo("drain-rate");
        assertThat(fired.getFirst().detail()).contains(DerivedMetrics.SLOPE_PER_MIN);
    }

    @Test
    @DisplayName("a slow drain does not fire, however long it goes on")
    void slowDrainStaysQuiet() {
        // 1 % a minute for twenty minutes — a normal discharge.
        for (int minute = 0; minute <= 20; minute++) {
            sendBattery(90 - minute, T0.plusSeconds(minute * 60L));
        }

        assertThat(alerts.readValuesToList()).isEmpty();
    }

    /**
     * The minimum-sample gate, which is what actually stops a dropout firing a trend rule.
     *
     * <p>Not the estimator. A least-squares fit is better than an endpoint difference but it is not
     * robust — on four samples the same dropout drags the slope to −9.1 %/min against a true
     * −1 %/min, and a dropout in the middle of four samples reverses its sign entirely. Four
     * samples cannot distinguish a fault from a glitch, and the honest response is to decline to
     * answer rather than to answer confidently.
     */
    @Test
    @DisplayName("too few samples publishes no slope at all, so the rule cannot fire")
    void tooFewSamplesCannotFire() {
        sendBattery(90, T0);
        sendBattery(89, T0.plusSeconds(60));
        sendBattery(88, T0.plusSeconds(120));
        sendBattery(60, T0.plusSeconds(180));

        assertThat(alerts.readValuesToList())
                .as("on four samples a −9.1 %/min fit would have fired; the gate is what stops it")
                .isEmpty();
    }

    @Test
    @DisplayName("once the outlier is outnumbered, the fit absorbs it")
    void outlierIsAbsorbedAtScale() {
        // Ten samples of a steady 1 %/min discharge, then one dropped reading 20 points low.
        for (int minute = 0; minute < 10; minute++) {
            sendBattery(90 - minute, T0.plusSeconds(minute * 60L));
        }
        sendBattery(60, T0.plusSeconds(600));

        assertThat(alerts.readValuesToList())
                .as("the fit lands near −1.9 %/min, well clear of the −5 %/min threshold")
                .isEmpty();
    }

    @Test
    @DisplayName("the trend alert clears through its own hysteresis band")
    void trendResolves() {
        for (int i = 0; i < 8; i++) {
            sendBattery(90 - 2 * i, T0.plusSeconds(i * 15L));
        }
        assertThat(alerts.readValuesToList()).hasSize(1);

        // The load drops and the discharge flattens. The window still holds the steep samples, so
        // the fit recovers as they age out rather than snapping back on the first flat reading.
        for (int i = 1; i <= 30; i++) {
            sendBattery(76, T0.plusSeconds(105 + i * 15L));
        }

        List<Alert> resolved = alerts.readValuesToList();
        assertThat(resolved).hasSize(1);
        assertThat(resolved.getFirst().kind()).isEqualTo(Alert.Kind.RESOLVED);
    }

    @Test
    @DisplayName("the window is derived from the rules, so a trend rule cannot miss its window")
    void windowIsDerivedFromTheRules() {
        assertThat(DerivedMetrics.windowedMetricsOf(drainRateRule()))
                .containsExactly(BATTERY);
    }

    private void sendBattery(int percent, Instant at) {
        long nanos = at.getEpochSecond() * 1_000_000_000L + at.getNano();
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_TENANT, bytes(TENANT)));
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_DEVICE, bytes(DEVICE)));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_MESSAGE_ID,
                        bytes(Integer.toString(MavlinkMetrics.MSG_SYS_STATUS))));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_RECEIVED_AT, bytes(Long.toString(nanos))));

        telemetry.pipeInput(
                new TestRecord<>(TENANT + '/' + DEVICE, sysStatusPayload(percent), headers, at));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sysStatusPayload(int remainingPercent) {
        ByteBuffer buffer = ByteBuffer.allocate(31).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(14, (short) 12_400);
        buffer.putShort(16, (short) 500);
        buffer.put(30, (byte) remainingPercent);
        return buffer.array();
    }
}
