package io.github.mustaffadnc.suru.ingest.dedup;

import io.github.mustaffadnc.suru.ingest.MessagePriority;
import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;

/**
 * Suppresses telemetry the platform has already seen.
 *
 * <p>Duplicates are real: a device retransmits, two network paths deliver the same datagram, a link
 * reconnects and replays what it had buffered. Downstream they inflate aggregates and corrupt
 * sequence-gap statistics.
 *
 * <p><b>The identity problem.</b> MAVLink's sequence number is eight bits and increments per
 * message across an endpoint's entire output, so at ArduPilot's typical rate it wraps roughly every
 * five seconds. Keying on {@code (device, sequence)} with any generous window therefore does not
 * identify duplicates — it identifies wrap-around, and drops perfectly good telemetry. The key must
 * include the message id and a payload digest, and the window must stay well inside the wrap
 * period.
 *
 * <p><b>Why critical messages are never deduplicated.</b> Even with a payload digest, low-entropy
 * messages collide with themselves: two HEARTBEATs a few seconds apart are frequently byte
 * identical, so a wrapped sequence number would make one look like a duplicate of the other. That
 * is the one message the platform must never lose — its absence is how a vehicle is declared gone.
 *
 * <p>The asymmetry settles it. A duplicate heartbeat costs nothing, because liveness is idempotent:
 * seeing the same one twice tells the platform exactly what seeing it once did. A dropped heartbeat
 * costs a false "telemetry lost" alarm. So {@link MessagePriority#CRITICAL} traffic bypasses
 * deduplication entirely, and everything else — where a duplicate skews an average and a rare false
 * positive does not — is filtered. Deduplication is applied where duplicates cost something and
 * withheld where suppression would cost more.
 *
 * <p>Implementations must be safe to call from several event loop threads.
 */
public interface DuplicateFilter extends AutoCloseable {

    /**
     * Whether this message has already been seen inside the window.
     *
     * <p>Best effort by construction: a filter that never errs would need unbounded memory and
     * perfect ordering. Implementations may miss a duplicate under contention; they must not
     * systematically suppress distinct messages.
     *
     * @param envelope the message
     * @return {@code true} if it should be suppressed
     */
    boolean isDuplicate(TelemetryEnvelope envelope);

    /**
     * Counters for what the filter has done.
     *
     * @return a snapshot
     */
    DuplicateFilterStats stats();

    @Override
    void close();

    /**
     * Whether a message is subject to deduplication at all.
     *
     * @param envelope the message
     * @return {@code false} for critical traffic, which always passes
     */
    static boolean isEligible(TelemetryEnvelope envelope) {
        return envelope.priority() != MessagePriority.CRITICAL;
    }

    /**
     * The identity of a message for deduplication purposes.
     *
     * <p>Combines device, message id, sequence and a digest of the payload. The digest is what
     * separates a genuine retransmission from a sequence number that has merely wrapped around
     * onto the same message type again.
     *
     * @param envelope the message
     * @return a 64-bit key
     */
    static long keyOf(TelemetryEnvelope envelope) {
        long hash = 0xcbf29ce484222325L; // FNV-1a 64-bit offset basis
        hash = fnv(hash, envelope.deviceId().hashCode());
        hash = fnv(hash, envelope.messageId());
        hash = fnv(hash, envelope.sequence());
        hash = fnv(hash, envelope.systemId());
        hash = fnv(hash, envelope.componentId());
        byte[] payload = envelope.payload();
        for (byte b : payload) {
            hash = (hash ^ (b & 0xFF)) * 0x100000001b3L;
        }
        return hash;
    }

    private static long fnv(long hash, int value) {
        long h = hash;
        for (int shift = 0; shift < 32; shift += 8) {
            h = (h ^ ((value >>> shift) & 0xFF)) * 0x100000001b3L;
        }
        return h;
    }

    /** A filter that suppresses nothing, for deployments that do not need deduplication. */
    static DuplicateFilter disabled() {
        return new DuplicateFilter() {
            @Override
            public boolean isDuplicate(TelemetryEnvelope envelope) {
                return false;
            }

            @Override
            public DuplicateFilterStats stats() {
                return new DuplicateFilterStats(0, 0, 0, 0);
            }

            @Override
            public void close() {
                // nothing held
            }
        };
    }
}
