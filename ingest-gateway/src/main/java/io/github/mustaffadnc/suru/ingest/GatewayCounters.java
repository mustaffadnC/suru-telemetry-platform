package io.github.mustaffadnc.suru.ingest;

import java.util.concurrent.atomic.LongAdder;

/**
 * Counters shared by every connection on a gateway.
 *
 * <p>{@code readPauses} is not merely diagnostic. It is the signal that separates "the gateway is
 * coping" from "the gateway is only coping because it keeps telling senders to wait": throughput
 * and shed counts both look healthy in the second case, and only this counter shows that headroom
 * has gone. A rising pause rate with zero shedding is the warning that arrives before any data is
 * lost.
 */
public final class GatewayCounters {

    private final LongAdder publishFailures = new LongAdder();
    private final LongAdder readPauses = new LongAdder();
    private final LongAdder connectionsAccepted = new LongAdder();
    private final LongAdder connectionsRejected = new LongAdder();

    /** Records a publication that failed downstream. */
    public void publishFailed() {
        publishFailures.increment();
    }

    /** Records a channel closing its read gate. */
    public void readPaused() {
        readPauses.increment();
    }

    /** Records an accepted connection. */
    public void connectionAccepted() {
        connectionsAccepted.increment();
    }

    /** Records a connection refused because its peer resolved to no tenant. */
    public void connectionRejected() {
        connectionsRejected.increment();
    }

    /**
     * Publications that failed downstream.
     *
     * @return the count
     */
    public long publishFailures() {
        return publishFailures.sum();
    }

    /**
     * How many times a channel stopped reading to let the downstream catch up.
     *
     * @return the count
     */
    public long readPauses() {
        return readPauses.sum();
    }

    /**
     * Connections accepted.
     *
     * @return the count
     */
    public long connectionsAccepted() {
        return connectionsAccepted.sum();
    }

    /**
     * Connections refused.
     *
     * @return the count
     */
    public long connectionsRejected() {
        return connectionsRejected.sum();
    }

    @Override
    public String toString() {
        return "connections=%d rejected=%d readPauses=%d publishFailures=%d"
                .formatted(
                        connectionsAccepted(),
                        connectionsRejected(),
                        readPauses(),
                        publishFailures());
    }
}
