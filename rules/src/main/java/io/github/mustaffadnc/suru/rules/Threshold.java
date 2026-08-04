package io.github.mustaffadnc.suru.rules;

import java.util.Objects;
import java.util.OptionalDouble;

/**
 * Fires when a metric crosses {@code fireAt}, and keeps holding until it crosses back past
 * {@code clearAt}.
 *
 * <p>The two thresholds are the hysteresis. A battery rule is written {@code BELOW, fireAt = 20,
 * clearAt = 25}: it fires under 20 % and does not stop holding until the reading is back above
 * 25 %. Between the two the condition simply keeps whatever answer it last gave, so a value
 * oscillating around 20 produces one alert rather than a burst.
 *
 * <p><b>A missing metric never holds.</b> If a device has not reported the metric at all, this
 * returns {@code false} rather than treating absence as a violation. Absence is a real condition
 * worth alerting on, but it is a different one, and conflating them means a rule about battery
 * level fires when the battery telemetry is merely missing — pointing the operator at the wrong
 * problem. {@link Staleness} is the rule for silence.
 *
 * @param metric the metric to test
 * @param comparison which side of the threshold is the violation
 * @param fireAt threshold at which an inactive alert starts holding
 * @param clearAt threshold at which an active alert stops holding
 */
public record Threshold(String metric, Comparison comparison, double fireAt, double clearAt)
        implements Condition {

    /** Which direction counts as a violation. */
    public enum Comparison {
        /** Violation when the value falls below the threshold — battery, fuel, signal. */
        BELOW,
        /** Violation when the value rises above the threshold — temperature, current, altitude. */
        ABOVE
    }

    /**
     * Rejects a hysteresis band that is the wrong way round.
     *
     * <p>For {@code BELOW}, clearing must need a <em>higher</em> reading than firing did. Given
     * {@code clearAt < fireAt} the alert would stop holding while the value is still worse than the
     * one that triggered it, which is not a narrower band — it is an alert that clears itself while
     * the fault is getting worse. The mistake is easy to make because both numbers are "the
     * threshold" in conversation, and it produces no error at runtime, just alerts that vanish.
     */
    public Threshold {
        Objects.requireNonNull(metric, "metric");
        Objects.requireNonNull(comparison, "comparison");
        if (!Double.isFinite(fireAt) || !Double.isFinite(clearAt)) {
            throw new IllegalArgumentException("thresholds must be finite");
        }
        boolean sane =
                switch (comparison) {
                    case BELOW -> clearAt >= fireAt;
                    case ABOVE -> clearAt <= fireAt;
                };
        if (!sane) {
            throw new IllegalArgumentException(
                    "hysteresis band is inverted for %s: fireAt=%s clearAt=%s — clearing must be the harder test"
                            .formatted(comparison, fireAt, clearAt));
        }
    }

    /**
     * A threshold with no hysteresis, firing and clearing at the same value.
     *
     * @param metric the metric to test
     * @param comparison which side of the threshold is the violation
     * @param at the threshold
     * @return the condition
     */
    public static Threshold at(String metric, Comparison comparison, double at) {
        return new Threshold(metric, comparison, at, at);
    }

    @Override
    public boolean holds(Observation observation, boolean active) {
        OptionalDouble value = observation.value(metric);
        if (value.isEmpty()) {
            return false;
        }
        double v = value.getAsDouble();
        double bound = active ? clearAt : fireAt;
        return switch (comparison) {
            case BELOW -> v < bound;
            case ABOVE -> v > bound;
        };
    }

    @Override
    public String describe(Observation observation) {
        OptionalDouble value = observation.value(metric);
        String reading = value.isEmpty() ? "no reading" : "%.3f".formatted(value.getAsDouble());
        return "%s %s (threshold %s %s)"
                .formatted(
                        metric,
                        reading,
                        comparison == Comparison.BELOW ? "<" : ">",
                        "%.3f".formatted(fireAt));
    }
}
