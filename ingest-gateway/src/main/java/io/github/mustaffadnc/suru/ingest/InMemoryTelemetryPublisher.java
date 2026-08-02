package io.github.mustaffadnc.suru.ingest;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final List<TelemetryEnvelope> published = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<CompletableFuture<Void>> stalled =
            new ConcurrentLinkedQueue<>();
    private final AtomicBoolean stalling = new AtomicBoolean();
    private final AtomicBoolean failing = new AtomicBoolean();

    @Override
    public CompletionStage<Void> publish(TelemetryEnvelope envelope) {
        published.add(envelope);

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
     * @return a live view; safe to iterate while publishing continues
     */
    public List<TelemetryEnvelope> published() {
        return published;
    }

    /**
     * How many publications are currently held open by a stall.
     *
     * @return the count
     */
    public int stalledCount() {
        return stalled.size();
    }

    /** Discards recorded messages. */
    public void clear() {
        published.clear();
    }

    @Override
    public void close() {
        resume();
    }
}
