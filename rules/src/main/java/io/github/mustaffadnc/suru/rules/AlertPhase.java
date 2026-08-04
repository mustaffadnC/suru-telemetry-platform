package io.github.mustaffadnc.suru.rules;

/**
 * Where a rule stands for one device.
 *
 * <p>The two waiting phases are what stop an alert being a report of the last sample. {@link
 * #PENDING} is the debounce before firing — a condition has to hold continuously, not merely have
 * been true once. {@link #RESOLVING} is the same discipline applied to recovery, and it is the one
 * usually left out: without it a single good reading in the middle of a fault closes the alert, and
 * the next bad reading opens a new one, so an operator watching a struggling vehicle sees a stream
 * of separate incidents instead of one continuing problem.
 *
 * <p>This mirrors the mission state machine in the ÇARGE firmware, where every transition is
 * debounced for the same reason: a state machine that reacts to instantaneous readings is a noise
 * amplifier.
 */
public enum AlertPhase {
    /** The condition does not hold and no alert is open. */
    INACTIVE,
    /** The condition holds but has not held long enough to fire. */
    PENDING,
    /** The alert is open. */
    FIRING,
    /** The condition has stopped holding, but not for long enough to close the alert. */
    RESOLVING;

    /**
     * Whether an alert is open in this phase, which is also the hysteresis band selector.
     *
     * <p>{@link #RESOLVING} counts as active: the alert is still open, and the condition must be
     * judged by the harder clearing threshold until it actually closes.
     *
     * @return {@code true} in {@link #FIRING} and {@link #RESOLVING}
     */
    public boolean alertOpen() {
        return this == FIRING || this == RESOLVING;
    }
}
