package io.github.mustaffadnc.suru.streams.notify;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.Severity;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Retry policy, permanent-failure handling, and the offset discipline.
 *
 * <p>A {@link MockConsumer} rather than a broker: what is under test is when the notifier commits
 * and when it gives up, and both are decisions this class makes rather than things Kafka does.
 */
class AlertNotifierTest {

    private static final String TOPIC = "alerts";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private MockConsumer<String, byte[]> consumer;
    private long nextOffset;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>("earliest");
        consumer.assign(List.of(PARTITION));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        nextOffset = 0;
    }

    private static Alert alert(String device) {
        return new Alert(
                "battery",
                "Battery low",
                "acme",
                device,
                Severity.CRITICAL,
                Alert.Kind.FIRED,
                Instant.parse("2026-08-04T12:00:00Z"),
                Instant.parse("2026-08-04T12:00:04Z"),
                Duration.ofSeconds(30),
                "battery low");
    }

    private void publish(Alert alert) throws Exception {
        consumer.addRecord(
                new ConsumerRecord<>(
                        TOPIC, 0, nextOffset++, alert.key(), MAPPER.writeValueAsBytes(alert)));
    }

    private void publishGarbage() {
        consumer.addRecord(
                new ConsumerRecord<>(TOPIC, 0, nextOffset++, "bad", new byte[] {'{', 'x'}));
    }

    /** A sink that fails a set number of times before succeeding. */
    private static final class FlakySink implements AlertSink {
        private final int failures;
        private final boolean retryable;
        private final AtomicInteger attempts = new AtomicInteger();
        private final List<Alert> accepted = new ArrayList<>();

        FlakySink(int failures, boolean retryable) {
            this.failures = failures;
            this.retryable = retryable;
        }

        @Override
        public void deliver(Alert alert) throws DeliveryException {
            if (attempts.incrementAndGet() <= failures) {
                throw new DeliveryException("failing on purpose", retryable, null);
            }
            accepted.add(alert);
        }

        @Override
        public String name() {
            return "flaky";
        }
    }

    private AlertNotifier notifier(AlertSink sink, AlertNotifier.RetryPolicy policy) {
        return new AlertNotifier(consumer, List.of(sink), policy, Duration.ofMillis(10));
    }

    private static AlertNotifier.RetryPolicy fastRetries(int attempts) {
        return new AlertNotifier.RetryPolicy(
                attempts, Duration.ofMillis(1), Duration.ofMillis(2));
    }

    @Test
    @DisplayName("a healthy sink receives the alert and the offset advances")
    void deliversAndCommits() throws Exception {
        FlakySink sink = new FlakySink(0, true);
        publish(alert("link/sys1"));

        try (AlertNotifier notifier = notifier(sink, fastRetries(4))) {
            assertThat(notifier.pollOnce()).isEqualTo(1);
            assertThat(sink.accepted).hasSize(1);
            assertThat(notifier.stats().delivered()).isEqualTo(1);
            assertThat(committedOffset()).isEqualTo(1);
        }
    }

    /** Reads the committed offset. Must be called before the notifier closes the consumer. */
    private long committedOffset() {
        return consumer.committed(java.util.Set.of(PARTITION)).get(PARTITION).offset();
    }

    @Test
    @DisplayName("a transient failure is retried and then succeeds")
    void retriesTransientFailures() throws Exception {
        FlakySink sink = new FlakySink(2, true);
        publish(alert("link/sys1"));

        try (AlertNotifier notifier = notifier(sink, fastRetries(4))) {
            notifier.pollOnce();

            assertThat(sink.accepted).hasSize(1);
            assertThat(sink.attempts).hasValue(3);
            assertThat(notifier.stats().retries()).isEqualTo(2);
            assertThat(notifier.stats().abandoned()).isZero();
        }
    }

    @Test
    @DisplayName("a permanent failure is not retried at all")
    void permanentFailureSkipsRetries() throws Exception {
        FlakySink sink = new FlakySink(99, false);
        publish(alert("link/sys1"));

        try (AlertNotifier notifier = notifier(sink, fastRetries(4))) {
            notifier.pollOnce();

            assertThat(sink.attempts)
                    .as("retrying a 400 burns the budget a transient failure would have needed")
                    .hasValue(1);
            assertThat(notifier.stats().retries()).isZero();
            assertThat(notifier.stats().abandoned()).isEqualTo(1);
        }
    }

    /**
     * The reason giving up is deliberate.
     *
     * <p>Retrying forever looks like the safe choice and is not: the offset only advances on
     * success, so one permanently undeliverable alert would block every alert behind it. Losing one
     * is bad; losing all the subsequent ones to protect it is worse.
     */
    @Test
    @DisplayName("an undeliverable alert is abandoned so the ones behind it still arrive")
    void abandonsRatherThanBlockingTheTopic() throws Exception {
        AtomicInteger seen = new AtomicInteger();
        AlertSink onlySecondWorks =
                new AlertSink() {
                    @Override
                    public void deliver(Alert alert) throws DeliveryException {
                        if (alert.deviceId().equals("link/sys1")) {
                            throw new DeliveryException("always fails", true, null);
                        }
                        seen.incrementAndGet();
                    }

                    @Override
                    public String name() {
                        return "picky";
                    }
                };

        publish(alert("link/sys1"));
        publish(alert("link/sys2"));

        try (AlertNotifier notifier = notifier(onlySecondWorks, fastRetries(3))) {
            notifier.pollOnce();

            assertThat(seen)
                    .as("the second alert must not be held hostage by the first")
                    .hasValue(1);
            assertThat(notifier.stats().abandoned()).isEqualTo(1);
            assertThat(committedOffset())
                    .as("the offset moves past the undeliverable alert, or nothing behind it ships")
                    .isEqualTo(2);
        }
    }

    @Test
    @DisplayName("an unreadable record is skipped rather than stopping the notifier")
    void skipsUndecodableRecords() throws Exception {
        FlakySink sink = new FlakySink(0, true);
        publishGarbage();
        publish(alert("link/sys2"));

        try (AlertNotifier notifier = notifier(sink, fastRetries(4))) {
            notifier.pollOnce();

            assertThat(notifier.stats().undecodable()).isEqualTo(1);
            assertThat(sink.accepted)
                    .as("a record that cannot be read will never become readable")
                    .hasSize(1);
        }
    }

    @Test
    @DisplayName("every sink gets the alert, and one failing does not stop the others")
    void deliversToEverySink() throws Exception {
        FlakySink good = new FlakySink(0, true);
        FlakySink broken = new FlakySink(99, false);
        publish(alert("link/sys1"));

        try (AlertNotifier notifier =
                new AlertNotifier(
                        consumer, List.of(broken, good), fastRetries(2), Duration.ofMillis(10))) {
            assertThat(notifier.pollOnce()).isEqualTo(1);

            assertThat(good.accepted).hasSize(1);
            assertThat(notifier.stats().delivered()).isEqualTo(1);
            assertThat(notifier.stats().abandoned()).isEqualTo(1);
        }
    }
}
