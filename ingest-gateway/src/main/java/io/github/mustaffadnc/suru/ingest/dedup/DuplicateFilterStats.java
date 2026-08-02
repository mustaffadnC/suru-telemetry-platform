package io.github.mustaffadnc.suru.ingest.dedup;

/**
 * What a {@link DuplicateFilter} has done.
 *
 * <p>{@code exempt} is reported rather than folded into {@code passed} because the two answer
 * different questions. A high {@code exempt} count is normal — heartbeats are frequent and are
 * never filtered. A {@code suppressed} count that climbs steadily means something upstream really
 * is sending twice, and is worth investigating rather than quietly absorbing.
 *
 * @param passed messages examined and allowed through
 * @param suppressed messages recognised as duplicates
 * @param exempt messages that bypassed the filter because they are critical
 * @param tracked identities currently held in the window
 */
public record DuplicateFilterStats(long passed, long suppressed, long exempt, long tracked) {

    /**
     * Fraction of examined messages that were suppressed.
     *
     * @return {@code 0.0..1.0}, or {@code 0.0} when nothing has been examined
     */
    public double suppressionRatio() {
        long examined = passed + suppressed;
        return examined == 0 ? 0.0 : (double) suppressed / examined;
    }

    @Override
    public String toString() {
        return "passed=%d suppressed=%d (%.2f%%) exempt=%d tracked=%d"
                .formatted(passed, suppressed, suppressionRatio() * 100.0, exempt, tracked);
    }
}
