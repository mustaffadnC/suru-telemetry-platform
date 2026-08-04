package io.github.mustaffadnc.suru.controlplane.command;

/**
 * Where a command stands.
 *
 * <p><b>{@link #REJECTED} and {@link #TIMED_OUT} are kept apart deliberately.</b> Rejected means the
 * vehicle received the command, understood it and refused — a successful round trip and a failed
 * command, and the operator knows exactly where they stand. Timed out means nobody knows: the
 * command may have been lost on the way out, executed with the ACK lost on the way back, or still
 * be sitting in a buffer. Folding both into one "failed" state would tell an operator that a
 * command definitely did not happen when the truth is that it might have.
 */
public enum CommandState {
    /** Accepted and durably recorded; the relay has not published it yet. */
    PENDING,
    /** Published towards the vehicle; no answer yet. */
    SENT,
    /** The vehicle accepted it. */
    ACKED,
    /** The vehicle received it and refused. */
    REJECTED,
    /** The window expired with no answer, and what happened is unknown. */
    TIMED_OUT;

    /**
     * Whether this state is still waiting on the vehicle.
     *
     * @return {@code true} for {@link #PENDING} and {@link #SENT}
     */
    public boolean awaitingAck() {
        return this == PENDING || this == SENT;
    }

    /**
     * Whether the command's outcome is settled.
     *
     * @return {@code true} once no further transition is expected
     */
    public boolean terminal() {
        return !awaitingAck();
    }
}
