package io.github.mustaffadnc.suru.ingest;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * A publisher that keeps everything in memory, and can be stalled on demand.
 *
 * <p>The stall is the point. Backpressure and shedding are only observable when the downstream
 * stops draining, and reproducing that against a real broker means killing a container and hoping
 * the timing lines up. Here {@link #stall()} holds every subsequent publication open, pressure
 * climbs deterministically, and the resulting behaviour is asserted without a sleep anywhere.
 *
 * <p>For tests and local development only.
 */
public final class InMemoryTelemetryPublisher implements TelemetryPublisher {

    // A ConcurrentLinkedQueue, emphatically not a CopyOnWriteArrayList. The first version used
    // the latter, whose add() copies the entire backing array: a load run of 677k messages
    // turned into ~10^11 element copies and the harness became the bottleneck. The first
    // throughput measurement it produced — 1,692 frames/s against a decoder that does 16.6M/s
    // — was measuring this class, not the gateway.
    private final ConcurrentLinkedQueue<TelemetryEnvelope> published = new ConcurrentLinkedQueue<>();
    private final LongAdder publishedCount = new LongAdder();
    private final ConcurrentLinkedQueue<CompletableFuture<Void>> stalled =
            new ConcurrentLinkedQueue<>();
    private final AtomicBoolean stalling = new AtomicBoolean();
    private final AtomicBoolean failing = new AtomicBoolean();
    private final AtomicBoolean recording = new AtomicBoolean(true);

    @Override
    public CompletionStage<Void> publish(TelemetryEnvelope envelope) {
        if (recording.get()) {
            published.add(envelope);
        }
        publishedCount.increment();

        if (failing.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("publisher is in induced-failure mode"));
        }
        if (stalling.get()) {
            CompletableFuture<Void> pending = new CompletableFuture<>();
            stalled.add(pending);
            return pending;
        }
        return CompletableFuture.completedFuture(null);
    }

    /** Holds every subsequent publication open, so in-flight work accumulates. */
    public void stall() {
        stalling.set(true);
    }

    /** Completes everything held open and returns to immediate completion. */
    public void resume() {
        stalling.set(false);
        CompletableFuture<Void> pending;
        while ((pending = stalled.poll()) != null) {
            pending.complete(null);
        }
    }

    /** Makes every subsequent publication fail immediately. */
    public void failEverything() {
        failing.set(true);
    }

    /** Stops inducing failures. */
    public void stopFailing() {
        failing.set(false);
    }

    /**
     * Everything handed to this publisher, in arrival order.
     *
     * <p>Builds a snapshot on each call. Fine for assertions over hundreds of messages; use
     * {@link #publishedCount()} in a load run rather than calling this in a polling loop.
     *
     * @return a snapshot of what has been published so far
     */
    public List<TelemetryEnvelope> published() {
        return List.copyOf(published);
    }

    /**
     * How many messages have been published.
     *
     * <p>Constant time, and unaffected by {@link #stopRecording()}.
     *
     * @return the count
     */
    public long publishedCount() {
        return publishedCount.sum();
    }

    /**
     * Stops retaining envelopes, keeping only the count.
     *
     * <p>For load runs: holding hundreds of thousands of envelopes measures the allocator and the
     * garbage collector as much as it measures the gateway.
     */
    public void stopRecording() {
        recording.set(false);
        published.clear();
    }

    /**
     * How many publications are currently held open by a stall.
     *
     * @return the count
     */
    public int stalledCount() {
        return stalled.size();
    }

    /** Discards recorded messages and resets the count. */
    public void clear() {
        published.clear();
        publishedCount.reset();
    }

    @Override
    public void close() {
        resume();
    }
}
