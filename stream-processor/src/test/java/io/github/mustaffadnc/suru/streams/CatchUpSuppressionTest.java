package io.github.mustaffadnc.suru.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.Rule;
import io.github.mustaffadnc.suru.rules.RuleEngine;
import io.github.mustaffadnc.suru.rules.Severity;
import io.github.mustaffadnc.suru.rules.Staleness;
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
 * The replay-versus-outage distinction, which is what makes wall-clock punctuation usable.
 *
 * <p>Both situations present the same obvious symptom — record timestamps far behind the wall clock
 * — and the platform has to respond to them in opposite ways. Getting this wrong in either
 * direction is a real failure: suppress too eagerly and a genuine outage goes unreported, suppress
 * too little and every restart pages the operator about the entire fleet.
 */
class CatchUpSuppressionTest {

    private static final String TELEMETRY_TOPIC = "telemetry.raw";
    private static final String ALERT_TOPIC = "alerts";
    private static final String TENANT = "acme";

    /** Well past the 15-second staleness limit, well past the 1-minute catch-up threshold. */
    private static final Instant NOW = Instant.parse("2026-08-04T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> telemetry;
    private TestOutputTopic<String, Alert> alerts;

    private static RuleEngine silenceOnly() {
        return new RuleEngine(
                List.of(
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suru-catchup-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");

        AlertTopology.Config config =
                new AlertTopology.Config(
                        TELEMETRY_TOPIC,
                        ALERT_TOPIC,
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(1),
                        false);

        driver = new TopologyTestDriver(AlertTopology.build(silenceOnly(), config), props, NOW);
        telemetry =
                driver.createInputTopic(
                        TELEMETRY_TOPIC, new StringSerializer(), new ByteArraySerializer());
        alerts =
                driver.createOutputTopic(
                        ALERT_TOPIC, new StringDeserializer(), JsonSerde.of(Alert.class).deserializer());
    }

    @AfterEach
    void stopDriver() {
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * Advances the clock one punctuation interval at a time.
     *
     * <p><b>{@code advanceWallClockTime} fires each wall-clock punctuator exactly once per call,
     * however far it moves the clock.</b> Advancing twenty seconds against a one-second schedule
     * produces one tick, not twenty — verified rather than assumed. A test that advanced in one
     * jump would therefore exercise a cadence production never has, and would hide anything whose
     * behaviour depends on what happened on the previous tick. The catch-up guard is exactly such a
     * thing: it needs a second tick to observe that the traffic stopped.
     */
    private void tick(int seconds) {
        for (int i = 0; i < seconds; i++) {
            driver.advanceWallClockTime(Duration.ofSeconds(1));
        }
    }

    @Test
    @DisplayName("outage: nothing arriving and the clock moving fires the alert")
    void outageFires() {
        send("link/sys1", NOW);

        tick(20);

        assertThat(alerts.readValuesToList())
                .as("no records consumed, so this is silence rather than a backlog")
                .hasSize(1);
    }

    @Test
    @DisplayName("replay: a backlog of old records arriving suppresses the alert")
    void replayIsSuppressed() {
        // A backlog two hours old, arriving quickly — the shape of a restart after downtime.
        Instant backlogStart = NOW.minus(Duration.ofHours(2));
        for (int i = 0; i < 20; i++) {
            send("link/sys" + (i % 5), backlogStart.plusSeconds(i));
            tick(1);
        }

        assertThat(alerts.readValuesToList())
                .as("every one of these devices is two hours 'stale' and none of them is missing")
                .isEmpty();
    }

    @Test
    @DisplayName("the backlog running out is what turns the alerts back on")
    void suppressionEndsWhenTheBacklogDoes() {
        Instant backlogStart = NOW.minus(Duration.ofHours(2));
        for (int i = 0; i < 10; i++) {
            send("link/sys1", backlogStart.plusSeconds(i));
            tick(1);
        }
        assertThat(alerts.readValuesToList()).isEmpty();

        // The backlog is exhausted: no more records, and the newest data is still two hours old.
        // The device genuinely has not been heard from, and now it should say so.
        tick(2);

        assertThat(alerts.readValuesToList())
                .as("having caught up to data two hours old means the device really is silent")
                .hasSize(1);
    }

    @Test
    @DisplayName("normal operation: current records arriving do not suppress anything")
    void currentTrafficDoesNotSuppress() {
        // Records arriving with current timestamps, then the device drops off.
        for (int i = 0; i < 5; i++) {
            send("link/sys1", NOW.plusSeconds(i));
            tick(1);
        }
        assertThat(alerts.readValuesToList()).isEmpty();

        tick(20);

        assertThat(alerts.readValuesToList())
                .as("lag was never large, so the guard never engaged")
                .hasSize(1);
    }

    @Test
    @DisplayName("lag alone does not suppress — that would reintroduce the stream-time bug")
    void lagWithoutTrafficDoesNotSuppress() {
        // One very old record and then nothing. Lag is enormous; traffic is zero.
        send("link/sys1", NOW.minus(Duration.ofHours(6)));

        tick(5);

        assertThat(alerts.readValuesToList())
                .as(
                        "suppressing on lag alone would mean a fleet that went quiet six hours ago"
                                + " never gets reported")
                .hasSize(1);
        // The first tick after that record is legitimately suppressed — it cannot yet tell a
        // backlog from an outage. One tick is the minimum possible delay, and the alert arrives on
        // the second.
    }

    private void send(String deviceId, Instant at) {
        long nanos = at.getEpochSecond() * 1_000_000_000L + at.getNano();
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_TENANT, bytes(TENANT)));
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_DEVICE, bytes(deviceId)));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_MESSAGE_ID,
                        bytes(Integer.toString(MavlinkMetrics.MSG_HEARTBEAT))));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_RECEIVED_AT, bytes(Long.toString(nanos))));

        telemetry.pipeInput(
                new TestRecord<>(TENANT + '/' + deviceId, new byte[9], headers, at));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
