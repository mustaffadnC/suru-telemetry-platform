package io.github.mustaffadnc.suru.ingest.dedup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mustaffadnc.suru.ingest.MessagePriority;
import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InMemoryDuplicateFilterTest {

    private static TelemetryEnvelope envelope(
            String device, int messageId, int sequence, MessagePriority priority, byte[] payload) {
        return new TelemetryEnvelope(
                "tenant",
                device,
                TelemetryEnvelope.SourceProtocol.MAVLINK,
                messageId,
                sequence,
                1,
                1,
                System.nanoTime(),
                priority,
                payload);
    }

    @Test
    @DisplayName("The same message arriving twice is suppressed the second time")
    void suppressesRepeats() {
        try (InMemoryDuplicateFilter filter = new InMemoryDuplicateFilter()) {
            TelemetryEnvelope first =
                    envelope("dev/sys1", 33, 7, MessagePriority.HIGH, new byte[] {1, 2, 3});
            TelemetryEnvelope again =
                    envelope("dev/sys1", 33, 7, MessagePriority.HIGH, new byte[] {1, 2, 3});

            assertThat(filter.isDuplicate(first)).isFalse();
            assertThat(filter.isDuplicate(again)).isTrue();
            assertThat(filter.stats().suppressed()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("Heartbeats are never suppressed, however often they repeat")
    void criticalTrafficIsExempt() {
        // A HEARTBEAT payload barely changes, so two of them a few seconds apart are often
        // byte identical. With an 8-bit sequence that wraps roughly every five seconds, a
        // content-addressed filter would eventually call a live heartbeat a duplicate — and
        // dropping one is precisely how the platform decides a healthy vehicle has gone.
        // Seeing the same heartbeat twice, by contrast, tells the platform nothing new and
        // costs nothing. The asymmetry is why this band bypasses the filter.
        try (InMemoryDuplicateFilter filter = new InMemoryDuplicateFilter()) {
            byte[] payload = new byte[9];
            for (int i = 0; i < 100; i++) {
                TelemetryEnvelope heartbeat =
                        envelope("dev/sys1", 0, 42, MessagePriority.CRITICAL, payload);
                assertThat(filter.isDuplicate(heartbeat)).as("attempt %d", i).isFalse();
            }
            assertThat(filter.stats().suppressed()).isZero();
            assertThat(filter.stats().exempt()).isEqualTo(100);
        }
    }

    @Test
    @DisplayName("A wrapped sequence number carrying different data is not a duplicate")
    void sequenceWrapIsNotADuplicate() {
        // MAVLink's sequence is 8-bit and wraps every 256 messages — about five seconds at
        // ArduPilot's rate. Keying on (device, sequence) alone would suppress the wrap, so the
        // payload digest has to be part of the identity.
        try (InMemoryDuplicateFilter filter = new InMemoryDuplicateFilter()) {
            TelemetryEnvelope earlier =
                    envelope("dev/sys1", 33, 7, MessagePriority.HIGH, new byte[] {1, 2, 3});
            TelemetryEnvelope afterWrap =
                    envelope("dev/sys1", 33, 7, MessagePriority.HIGH, new byte[] {9, 9, 9});

            assertThat(filter.isDuplicate(earlier)).isFalse();
            assertThat(filter.isDuplicate(afterWrap)).isFalse();
            assertThat(filter.stats().suppressed()).isZero();
        }
    }

    @Test
    @DisplayName("Two devices sending identical bytes do not shadow each other")
    void devicesAreIndependent() {
        try (InMemoryDuplicateFilter filter = new InMemoryDuplicateFilter()) {
            byte[] payload = {4, 5, 6};
            assertThat(
                            filter.isDuplicate(
                                    envelope("linkA/sys1", 30, 3, MessagePriority.HIGH, payload)))
                    .isFalse();
            assertThat(
                            filter.isDuplicate(
                                    envelope("linkB/sys1", 30, 3, MessagePriority.HIGH, payload)))
                    .isFalse();
            assertThat(filter.stats().suppressed()).isZero();
        }
    }

    @Test
    @DisplayName("Once the window closes the same identity is accepted again")
    void windowExpires() {
        try (InMemoryDuplicateFilter filter =
                new InMemoryDuplicateFilter(Duration.ofMillis(150), 1000)) {
            TelemetryEnvelope message =
                    envelope("dev/sys1", 33, 7, MessagePriority.NORMAL, new byte[] {1});

            assertThat(filter.isDuplicate(message)).isFalse();
            assertThat(filter.isDuplicate(message)).isTrue();

            // Expiry is what stops the window outliving MAVLink's sequence wrap and turning
            // legitimate reuse into suppression.
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> !filter.isDuplicate(message));
        }
    }

    @Test
    @DisplayName("The tracked set stays bounded under a flood of distinct identities")
    void staysBounded() {
        int cap = 500;
        try (InMemoryDuplicateFilter filter =
                new InMemoryDuplicateFilter(Duration.ofMinutes(5), cap)) {
            for (int i = 0; i < 20_000; i++) {
                filter.isDuplicate(
                        envelope(
                                "dev/sys1",
                                33,
                                i & 0xFF,
                                MessagePriority.NORMAL,
                                new byte[] {(byte) i, (byte) (i >> 8), (byte) (i >> 16)}));
            }
            // An unbounded map keyed by message content is a memory leak that any sender can
            // trigger just by varying its payload. Overrunning the cap costs a missed
            // duplicate, never a suppressed distinct message.
            assertThat(filter.stats().tracked()).isLessThanOrEqualTo(cap + 1L);
            assertThat(filter.stats().suppressed()).isZero();
        }
    }

    @Test
    @DisplayName("The disabled filter suppresses nothing")
    void disabledPassesEverything() {
        try (DuplicateFilter filter = DuplicateFilter.disabled()) {
            TelemetryEnvelope message =
                    envelope("dev/sys1", 33, 7, MessagePriority.NORMAL, new byte[] {1});
            assertThat(filter.isDuplicate(message)).isFalse();
            assertThat(filter.isDuplicate(message)).isFalse();
        }
    }
}
