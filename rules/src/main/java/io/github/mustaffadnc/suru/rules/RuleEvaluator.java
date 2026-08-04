package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * The alert state machine: one pure function from (rule, previous state, observation) to the next
 * state and possibly an alert.
 *
 * <p><b>Deliberately stateless and clock-free.</b> Time enters only through {@link
 * Observation#at()}, so every debounce edge — fires exactly at the boundary, does not fire one
 * instant before, survives a momentary recovery — is an ordinary assertion with hand-written
 * instants rather than a test that sleeps and hopes. The corresponding Kafka Streams processor owns
 * the state store and the clock; this class owns the semantics, and the two are testable apart.
 *
 * <p>The transition table is small enough to state in full, and {@code RuleEvaluatorTest} covers
 * every edge of it:
 *
 * <pre>
 *   INACTIVE  + holds      → PENDING   (or FIRING when forDuration is zero)
 *   INACTIVE  + clear      → INACTIVE
 *   PENDING   + holds, ripe→ FIRING    → emits FIRED
 *   PENDING   + holds      → PENDING
 *   PENDING   + clear      → INACTIVE  emits nothing — this is the debounce earning its keep
 *   FIRING    + holds      → FIRING
 *   FIRING    + clear      → RESOLVING (or INACTIVE when clearDuration is zero)
 *   RESOLVING + holds      → FIRING    emits nothing — the alert never closed
 *   RESOLVING + clear, ripe→ INACTIVE  → emits RESOLVED
 *   RESOLVING + clear      → RESOLVING
 * </pre>
 *
 * <p>The two rows that emit nothing are the ones worth reading twice. {@code PENDING + clear} is
 * the whole point of debouncing: a condition that was briefly true never becomes an alert.
 * {@code RESOLVING + holds} is its mirror: a fault that briefly looked better does not produce a
 * second incident when it comes back, and the operator continues to see the one they already have.
 */
public final class RuleEvaluator {

    private RuleEvaluator() {}

    /**
     * The outcome of one evaluation.
     *
     * @param state the state to carry forward
     * @param alert an alert to publish, when this step opened or closed one
     */
    public record Evaluation(AlertState state, Optional<Alert> alert) {

        private static Evaluation quiet(AlertState state) {
            return new Evaluation(state, Optional.empty());
        }

        private static Evaluation emitting(AlertState state, Alert alert) {
            return new Evaluation(state, Optional.of(alert));
        }
    }

    /**
     * Advances one rule for one device.
     *
     * @param rule the rule to evaluate
     * @param previous the state from the last evaluation, or {@link AlertState#initial}
     * @param observation the device's current state
     * @return the next state, and an alert when this step opened or closed one
     */
    public static Evaluation step(Rule rule, AlertState previous, Observation observation) {
        Instant now = observation.at();
        boolean holds = rule.condition().holds(observation, previous.alertOpen());

        return holds
                ? whileHolding(rule, previous, observation, now)
                : whileClear(rule, previous, observation, now);
    }

    private static Evaluation whileHolding(
            Rule rule, AlertState previous, Observation observation, Instant now) {
        switch (previous.phase()) {
            case FIRING -> {
                return Evaluation.quiet(previous);
            }
            case RESOLVING -> {
                // The condition came back before the alert closed. Re-arm silently and keep the
                // original onset: this is the same fault continuing, not a new one.
                return Evaluation.quiet(
                        new AlertState(AlertPhase.FIRING, now, previous.conditionSince()));
            }
            case INACTIVE, PENDING -> {
                Instant onset =
                        previous.phase() == AlertPhase.INACTIVE ? now : previous.conditionSince();
                if (ripe(onset, now, rule.forDuration())) {
                    return Evaluation.emitting(
                            new AlertState(AlertPhase.FIRING, now, onset),
                            new Alert(
                                    rule.id(),
                                    rule.name(),
                                    observation.tenantId(),
                                    observation.deviceId(),
                                    rule.severity(),
                                    Alert.Kind.FIRED,
                                    onset,
                                    now,
                                    rule.forDuration(),
                                    rule.condition().describe(observation)));
                }
                Instant since = previous.phase() == AlertPhase.INACTIVE ? now : previous.since();
                return Evaluation.quiet(new AlertState(AlertPhase.PENDING, since, onset));
            }
        }
        throw new AssertionError("unreachable phase " + previous.phase());
    }

    private static Evaluation whileClear(
            Rule rule, AlertState previous, Observation observation, Instant now) {
        switch (previous.phase()) {
            case INACTIVE -> {
                return Evaluation.quiet(previous);
            }
            case PENDING -> {
                // Never fired, so nothing to resolve. The debounce did exactly what it is for.
                return Evaluation.quiet(new AlertState(AlertPhase.INACTIVE, now, now));
            }
            case FIRING, RESOLVING -> {
                Instant clearedAt =
                        previous.phase() == AlertPhase.FIRING ? now : previous.since();
                if (ripe(clearedAt, now, rule.clearDuration())) {
                    return Evaluation.emitting(
                            new AlertState(AlertPhase.INACTIVE, now, now),
                            new Alert(
                                    rule.id(),
                                    rule.name(),
                                    observation.tenantId(),
                                    observation.deviceId(),
                                    rule.severity(),
                                    Alert.Kind.RESOLVED,
                                    clearedAt,
                                    now,
                                    rule.clearDuration(),
                                    rule.condition().describe(observation)));
                }
                return Evaluation.quiet(
                        new AlertState(AlertPhase.RESOLVING, clearedAt, previous.conditionSince()));
            }
        }
        throw new AssertionError("unreachable phase " + previous.phase());
    }

    /**
     * Whether enough time has passed.
     *
     * <p>The boundary is inclusive: a 30-second debounce fires at exactly 30 seconds, not at the
     * first sample after it. With an exclusive test a device reporting on a period that divides the
     * debounce evenly — which is the normal case, since both are usually round numbers — waits a
     * whole extra sample interval every time.
     */
    private static boolean ripe(Instant start, Instant now, Duration required) {
        return Duration.between(start, now).compareTo(required) >= 0;
    }
}
