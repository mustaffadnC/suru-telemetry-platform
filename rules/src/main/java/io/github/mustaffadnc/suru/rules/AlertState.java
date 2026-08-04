package io.github.mustaffadnc.suru.rules;

import java.time.Instant;
import java.util.Objects;

/**
 * Where one rule stands for one device, and since when.
 *
 * <p>{@code since} is the instant the current phase was entered, and it carries the whole debounce:
 * {@link RuleEvaluator} fires by comparing it against the rule's {@code forDuration} rather than by
 * counting samples. Counting samples would tie the debounce to the telemetry rate, so the same rule
 * would take one second on a 10 Hz device and ten on a 1 Hz one.
 *
 * <p>{@code conditionSince} survives the {@link AlertPhase#PENDING} to {@link AlertPhase#FIRING}
 * transition so the alert can report when the fault actually started, not when the debounce
 * expired. Those differ by exactly {@code forDuration}, and reporting the later one would make
 * every measured detection latency include a delay that was deliberate.
 *
 * @param phase where the rule stands
 * @param since when this phase was entered
 * @param conditionSince when the condition most recently started holding
 */
public record AlertState(AlertPhase phase, Instant since, Instant conditionSince) {

    /** Validates that a state carries the instants its phase needs. */
    public AlertState {
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(since, "since");
        Objects.requireNonNull(conditionSince, "conditionSince");
    }

    /**
     * The starting state for a rule that has never been evaluated.
     *
     * @param at the current instant
     * @return an inactive state
     */
    public static AlertState initial(Instant at) {
        return new AlertState(AlertPhase.INACTIVE, at, at);
    }

    /**
     * Whether an alert is currently open.
     *
     * @return {@code true} in {@link AlertPhase#FIRING} and {@link AlertPhase#RESOLVING}
     */
    public boolean alertOpen() {
        return phase.alertOpen();
    }
}
