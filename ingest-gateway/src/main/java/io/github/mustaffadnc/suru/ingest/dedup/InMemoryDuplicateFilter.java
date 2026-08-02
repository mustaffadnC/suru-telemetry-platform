package io.github.mustaffadnc.suru.ingest.dedup;

import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Deduplication within a single gateway process.
 *
 * <p>Holds recently seen identities with an expiry, in a map bounded by entry count. Both bounds
 * matter: the window must stay well inside MAVLink's sequence wrap (see {@link DuplicateFilter}),
 * and the size cap keeps a burst of distinct traffic — or a hostile sender varying its payload —
 * from growing the map without limit.
 *
 * <p>This is the right choice for a single gateway instance and the wrong one for several: two
 * gateways behind a load balancer each keep their own window and neither sees the other's traffic,
 * so a device that reconnects to a different instance is not deduplicated. {@link
 * RedisDuplicateFilter} exists for that case and costs a network round trip per message to get it.
 */
public final class InMemoryDuplicateFilter implements DuplicateFilter {

    /** Default retention window, comfortably inside MAVLink's ~5 s sequence wrap. */
    public static final Duration DEFAULT_WINDOW = Duration.ofSeconds(2);

    /** Default cap on tracked identities. */
    public static final int DEFAULT_MAX_ENTRIES = 500_000;

    private final ConcurrentHashMap<Long, Long> seen = new ConcurrentHashMap<>();
    private final long windowNanos;
    private final int maxEntries;

    private final LongAdder passed = new LongAdder();
    private final LongAdder suppressed = new LongAdder();
    private final LongAdder exempt = new LongAdder();

    /** Creates a filter with the default window and size cap. */
    public InMemoryDuplicateFilter() {
        this(DEFAULT_WINDOW, DEFAULT_MAX_ENTRIES);
    }

    /**
     * Creates a filter.
     *
     * @param window how long an identity is remembered
     * @param maxEntries cap on tracked identities
     * @throws IllegalArgumentException if either bound is not positive
     */
    public InMemoryDuplicateFilter(Duration window, int maxEntries) {
        if (window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("window must be positive, was " + window);
        }
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive, was " + maxEntries);
        }
        this.windowNanos = window.toNanos();
        this.maxEntries = maxEntries;
    }

    @Override
    public boolean isDuplicate(TelemetryEnvelope envelope) {
        if (!DuplicateFilter.isEligible(envelope)) {
            exempt.increment();
            return false;
        }

        long key = DuplicateFilter.keyOf(envelope);
        long now = System.nanoTime();
        long expiry = now + windowNanos;

        Long previous = seen.putIfAbsent(key, expiry);
        if (previous == null) {
            passed.increment();
            evictIfCrowded(now);
            return false;
        }
        if (previous - now <= 0) {
            // Present but stale: the identity is being reused after its window closed, which is
            // exactly what sequence wrap-around looks like. Refresh rather than suppress.
            seen.put(key, expiry);
            passed.increment();
            return false;
        }
        suppressed.increment();
        return true;
    }

    private void evictIfCrowded(long now) {
        if (seen.size() <= maxEntries) {
            return;
        }
        // Sweep what has expired first; only if that frees nothing does the cap force out live
        // entries, and losing those merely lets a duplicate through — never suppresses a
        // distinct message.
        Iterator<Map.Entry<Long, Long>> it = seen.entrySet().iterator();
        while (it.hasNext() && seen.size() > maxEntries) {
            Map.Entry<Long, Long> entry = it.next();
            if (entry.getValue() - now <= 0) {
                it.remove();
            }
        }
        it = seen.entrySet().iterator();
        while (it.hasNext() && seen.size() > maxEntries) {
            it.next();
            it.remove();
        }
    }

    @Override
    public DuplicateFilterStats stats() {
        return new DuplicateFilterStats(
                passed.sum(), suppressed.sum(), exempt.sum(), seen.size());
    }

    @Override
    public void close() {
        seen.clear();
    }
}
