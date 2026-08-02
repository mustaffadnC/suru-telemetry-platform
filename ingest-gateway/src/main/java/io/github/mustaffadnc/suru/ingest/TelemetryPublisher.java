package io.github.mustaffadnc.suru.ingest;

import java.util.concurrent.CompletionStage;

/**
 * Where admitted telemetry goes.
 *
 * <p>Publication is asynchronous, and the returned stage is what admission control measures: it
 * completes when the downstream has durably taken the message, so outstanding stages are exactly
 * the gateway's in-flight work. A publisher that completed its stage on hand-off rather than on
 * acknowledgement would report no pressure however badly the broker was struggling.
 */
public interface TelemetryPublisher extends AutoCloseable {

    /**
     * Publishes one message.
     *
     * @param envelope the message
     * @return a stage completing when the downstream has accepted it, or completing exceptionally
     *     if it could not be delivered
     */
    CompletionStage<Void> publish(TelemetryEnvelope envelope);

    /** Flushes and releases resources. */
    @Override
    void close();
}
