package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.util.Objects;

/**
 * Fires when a device has been silent longer than {@code maxSilence}.
 *
 * <p><b>This is the one condition that cannot be evaluated by an arriving record.</b> Every other
 * rule is a function of data that showed up; this one is a function of data that did not. A
 * processor that only runs when a message arrives can never fire it, because the event it is
 * looking for is the absence of the event that would trigger the check. It needs to be evaluated on
 * a timer instead — see the punctuation notes on the stream topology.
 *
 * <p>Hysteresis is expressed in time rather than in a second threshold: {@code clearAfter} is how
 * recently the device must have been heard from for an <em>active</em> alert to stop holding. With
 * both set to the same value, a device transmitting right at the limit flaps. The default gives
 * clearing a shorter fuse than firing, so recovery has to be convincing.
 *
 * @param maxSilence how long a device may be quiet before this holds
 * @param clearAfter how recently an active alert needs to hear from the device to stop holding
 */
public record Staleness(Duration maxSilence, Duration clearAfter) implements Condition {

    /** Rejects a band that would clear later than it fires, which could never stop holding. */
    public Staleness {
        Objects.requireNonNull(maxSilence, "maxSilence");
        Objects.requireNonNull(clearAfter, "clearAfter");
        if (maxSilence.isNegative() || maxSilence.isZero()) {
            throw new IllegalArgumentException("maxSilence must be positive");
        }
        if (clearAfter.isNegative() || clearAfter.isZero()) {
            throw new IllegalArgumentException("clearAfter must be positive");
        }
        if (clearAfter.compareTo(maxSilence) > 0) {
            throw new IllegalArgumentException(
                    "clearAfter %s exceeds maxSilence %s — the alert could never clear"
                            .formatted(clearAfter, maxSilence));
        }
    }

    /**
     * A staleness condition whose alert clears once the device is heard from at half the firing
     * gap.
     *
     * @param maxSilence how long a device may be quiet before this holds
     * @return the condition
     */
    public static Staleness after(Duration maxSilence) {
        return new Staleness(maxSilence, maxSilence.dividedBy(2));
    }

    @Override
    public boolean holds(Observation observation, boolean active) {
        Duration limit = active ? clearAfter : maxSilence;
        return observation.silence().compareTo(limit) > 0;
    }

    @Override
    public String describe(Observation observation) {
        return "silent for %s (limit %s)".formatted(observation.silence(), maxSilence);
    }
}
