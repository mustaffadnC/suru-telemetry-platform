package io.github.mustaffadnc.suru.streams.notify;

import io.github.mustaffadnc.suru.rules.Alert;

/**
 * Somewhere an alert can be delivered.
 *
 * <p>Implementations are expected to be synchronous and to fail fast: {@link AlertNotifier} owns
 * the retry policy, the backoff and the decision about when to give up, so a sink that retried
 * internally would multiply those and turn one slow endpoint into a stalled notifier.
 */
public interface AlertSink {

    /**
     * Delivers one alert.
     *
     * @param alert the alert
     * @throws DeliveryException when delivery failed
     */
    void deliver(Alert alert) throws DeliveryException;

    /**
     * A name for logs and counters.
     *
     * @return the sink's name
     */
    String name();

    /** Delivery failed, and whether trying again could help. */
    final class DeliveryException extends Exception {

        private static final long serialVersionUID = 1L;

        private final boolean retryable;

        /**
         * Creates a failure.
         *
         * @param message what went wrong
         * @param retryable whether another attempt could succeed
         * @param cause the underlying failure, or {@code null}
         */
        public DeliveryException(String message, boolean retryable, Throwable cause) {
            super(message, cause);
            this.retryable = retryable;
        }

        /**
         * Whether another attempt could succeed.
         *
         * <p>The distinction is the whole reason this type exists. A 500 or a connection reset is
         * worth retrying; a 400 means the request is wrong and will be wrong every time, and
         * retrying it burns the budget that a genuinely transient failure needed.
         *
         * @return {@code true} when a retry is worth attempting
         */
        public boolean retryable() {
            return retryable;
        }
    }
}
