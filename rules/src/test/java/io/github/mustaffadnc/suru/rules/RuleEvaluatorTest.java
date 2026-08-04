package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Every edge of the alert state machine.
 *
 * <p>The phase-4 acceptance criterion is total coverage of the transition table, so this class
 * asserts it rather than asserting it in a comment: each step records the edge it traversed, and
 * {@link #everyTransitionWasExercised()} fails if any row of the table went untested. Adding a row
 * to {@link RuleEvaluator} without testing it breaks the build, which is the only version of
 * "100 % state coverage" that stays true after the person who wrote it moves on.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RuleEvaluatorTest {

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");
    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";
    private static final String BATTERY = "power.battery_remaining_pct";

    /** Every edge {@link RuleEvaluator} can traverse. Kept in sync by the coverage assertion. */
    private static final Set<String> TRANSITIONS =
            new TreeSet<>(
                    Set.of(
                            "INACTIVE+holds→PENDING",
                            "INACTIVE+holds→FIRING",
                            "INACTIVE+clear→INACTIVE",
                            "PENDING+holds→PENDING",
                            "PENDING+holds→FIRING",
                            "PENDING+clear→INACTIVE",
                            "FIRING+holds→FIRING",
                            "FIRING+clear→RESOLVING",
                            "FIRING+clear→INACTIVE",
                            "RESOLVING+holds→FIRING",
                            "RESOLVING+clear→RESOLVING",
                            "RESOLVING+clear→INACTIVE"));

    private final Set<String> covered = new LinkedHashSet<>();

    /** Battery below 20 %, clearing above 25 %, 30 s to fire and 10 s to clear. */
    private static Rule batteryRule() {
        return new Rule(
                "rule-battery",
                "Battery low",
                TENANT,
                Rule.ALL_DEVICES,
                new Threshold(BATTERY, Threshold.Comparison.BELOW, 20.0, 25.0),
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Severity.CRITICAL);
    }

    private static Observation battery(double percent, Instant at) {
        return new Observation(TENANT, DEVICE, Map.of(BATTERY, percent), at, at);
    }

    /** Steps the machine and records which edge was taken. */
    private RuleEvaluator.Evaluation step(Rule rule, AlertState before, Observation observation) {
        RuleEvaluator.Evaluation evaluation = RuleEvaluator.step(rule, before, observation);
        boolean holds = rule.condition().holds(observation, before.alertOpen());
        covered.add(
                "%s+%s→%s"
                        .formatted(
                                before.phase(),
                                holds ? "holds" : "clear",
                                evaluation.state().phase()));
        return evaluation;
    }

    @Test
    @DisplayName("a healthy reading changes nothing")
    void healthyReadingIsInert() {
        Rule rule = batteryRule();
        AlertState state = AlertState.initial(T0);

        RuleEvaluator.Evaluation evaluation = step(rule, state, battery(80.0, T0));

        assertThat(evaluation.state().phase()).isEqualTo(AlertPhase.INACTIVE);
        assertThat(evaluation.alert()).isEmpty();
    }

    @Test
    @DisplayName("a breach starts the debounce instead of firing")
    void breachStartsDebounce() {
        Rule rule = batteryRule();

        RuleEvaluator.Evaluation evaluation =
                step(rule, AlertState.initial(T0), battery(15.0, T0));

        assertThat(evaluation.state().phase()).isEqualTo(AlertPhase.PENDING);
        assertThat(evaluation.state().conditionSince()).isEqualTo(T0);
        assertThat(evaluation.alert()).as("nothing is published until the debounce expires").isEmpty();
    }

    @Test
    @DisplayName("the alert fires exactly at the debounce boundary, not one sample later")
    void firesAtTheBoundary() {
        Rule rule = batteryRule();
        AlertState state = step(rule, AlertState.initial(T0), battery(15.0, T0)).state();

        // One instant short: still pending.
        RuleEvaluator.Evaluation justShort =
                step(rule, state, battery(15.0, T0.plusSeconds(30).minusNanos(1)));
        assertThat(justShort.state().phase()).isEqualTo(AlertPhase.PENDING);
        assertThat(justShort.alert()).isEmpty();

        // Exactly at the boundary: fires. An exclusive test here would cost a device reporting
        // on a period that divides the debounce a whole extra interval, every time.
        RuleEvaluator.Evaluation atBoundary =
                step(rule, justShort.state(), battery(15.0, T0.plusSeconds(30)));
        assertThat(atBoundary.state().phase()).isEqualTo(AlertPhase.FIRING);
        assertThat(atBoundary.alert()).isPresent();
        assertThat(atBoundary.alert().orElseThrow().kind()).isEqualTo(Alert.Kind.FIRED);
    }

    @Test
    @DisplayName("a breach that clears before the debounce expires never becomes an alert")
    void debounceSwallowsTransientBreach() {
        Rule rule = batteryRule();
        AlertState pending = step(rule, AlertState.initial(T0), battery(15.0, T0)).state();

        RuleEvaluator.Evaluation recovered =
                step(rule, pending, battery(80.0, T0.plusSeconds(5)));

        assertThat(recovered.state().phase()).isEqualTo(AlertPhase.INACTIVE);
        assertThat(recovered.alert())
                .as("no FIRED, and therefore no RESOLVED either — the incident never existed")
                .isEmpty();
    }

    @Test
    @DisplayName("the fired alert reports when the fault began, not when the debounce expired")
    void alertReportsOnsetNotExpiry() {
        Rule rule = batteryRule();
        AlertState pending = step(rule, AlertState.initial(T0), battery(15.0, T0)).state();

        Instant firedAt = T0.plusSeconds(34);
        Alert alert = step(rule, pending, battery(15.0, firedAt)).alert().orElseThrow();

        assertThat(alert.conditionSince()).isEqualTo(T0);
        assertThat(alert.at()).isEqualTo(firedAt);
        assertThat(alert.detectionLatency()).isEqualTo(Duration.ofSeconds(34));
        assertThat(alert.incidentalLatency())
                .as("30 s of the 34 was the configured debounce; only 4 s is the platform's")
                .isEqualTo(Duration.ofSeconds(4));
    }

    @Test
    @DisplayName("hysteresis keeps the alert open between the two thresholds")
    void hysteresisHoldsBetweenThresholds() {
        Rule rule = batteryRule();
        AlertState firing = fireIt(rule);

        // 22 % is above the firing threshold of 20 but below the clearing threshold of 25.
        RuleEvaluator.Evaluation between =
                step(rule, firing, battery(22.0, T0.plusSeconds(60)));

        assertThat(between.state().phase())
                .as("without hysteresis this would start resolving and then re-fire on the next dip")
                .isEqualTo(AlertPhase.FIRING);
        assertThat(between.alert()).isEmpty();
    }

    @Test
    @DisplayName("recovery has to hold too, and a blip does not close the alert")
    void recoveryIsDebouncedAsWell() {
        Rule rule = batteryRule();
        AlertState firing = fireIt(rule);

        AlertState resolving =
                step(rule, firing, battery(90.0, T0.plusSeconds(60))).state();
        assertThat(resolving.phase()).isEqualTo(AlertPhase.RESOLVING);

        // Still resolving five seconds in.
        RuleEvaluator.Evaluation midway =
                step(rule, resolving, battery(90.0, T0.plusSeconds(65)));
        assertThat(midway.state().phase()).isEqualTo(AlertPhase.RESOLVING);
        assertThat(midway.alert()).isEmpty();

        // The fault returns before the clear debounce expires: back to firing, silently.
        RuleEvaluator.Evaluation relapse =
                step(rule, midway.state(), battery(10.0, T0.plusSeconds(67)));
        assertThat(relapse.state().phase()).isEqualTo(AlertPhase.FIRING);
        assertThat(relapse.alert())
                .as("one continuing incident, not a second one — the alert never closed")
                .isEmpty();
        assertThat(relapse.state().conditionSince())
                .as("the original onset survives a relapse")
                .isEqualTo(T0);
    }

    @Test
    @DisplayName("sustained recovery closes the alert")
    void sustainedRecoveryResolves() {
        Rule rule = batteryRule();
        AlertState firing = fireIt(rule);

        AlertState resolving =
                step(rule, firing, battery(90.0, T0.plusSeconds(60))).state();
        RuleEvaluator.Evaluation resolved =
                step(rule, resolving, battery(90.0, T0.plusSeconds(70)));

        assertThat(resolved.state().phase()).isEqualTo(AlertPhase.INACTIVE);
        assertThat(resolved.alert()).isPresent();
        Alert alert = resolved.alert().orElseThrow();
        assertThat(alert.kind()).isEqualTo(Alert.Kind.RESOLVED);
        assertThat(alert.conditionSince())
                .as("a resolved alert dates from when the fault stopped, not when it started")
                .isEqualTo(T0.plusSeconds(60));
    }

    @Test
    @DisplayName("a sustained fault does not re-fire on every sample")
    void firingIsIdempotent() {
        Rule rule = batteryRule();
        AlertState firing = fireIt(rule);

        for (int second = 60; second < 70; second++) {
            RuleEvaluator.Evaluation next =
                    step(rule, firing, battery(5.0, T0.plusSeconds(second)));
            assertThat(next.alert()).isEmpty();
            firing = next.state();
        }
        assertThat(firing.phase()).isEqualTo(AlertPhase.FIRING);
    }

    @Test
    @DisplayName("zero debounce fires and resolves on the first sample")
    void zeroDebounceIsImmediate() {
        Rule rule =
                new Rule(
                        "rule-instant",
                        "Instant",
                        TENANT,
                        Rule.ALL_DEVICES,
                        Threshold.at(BATTERY, Threshold.Comparison.BELOW, 20.0),
                        Duration.ZERO,
                        Duration.ZERO,
                        Severity.WARNING);

        RuleEvaluator.Evaluation fired =
                step(rule, AlertState.initial(T0), battery(10.0, T0));
        assertThat(fired.state().phase()).isEqualTo(AlertPhase.FIRING);
        assertThat(fired.alert().orElseThrow().kind()).isEqualTo(Alert.Kind.FIRED);

        RuleEvaluator.Evaluation resolved =
                step(rule, fired.state(), battery(80.0, T0.plusSeconds(1)));
        assertThat(resolved.state().phase()).isEqualTo(AlertPhase.INACTIVE);
        assertThat(resolved.alert().orElseThrow().kind()).isEqualTo(Alert.Kind.RESOLVED);
    }

    /** Drives a rule to FIRING, recording the edges on the way. */
    private AlertState fireIt(Rule rule) {
        AlertState pending = step(rule, AlertState.initial(T0), battery(15.0, T0)).state();
        return step(rule, pending, battery(15.0, T0.plusSeconds(30))).state();
    }

    @AfterAll
    void everyTransitionWasExercised() {
        assertThat(covered)
                .as("transitions never reached by any test in this class")
                .containsAll(TRANSITIONS);
        assertThat(TRANSITIONS)
                .as("a transition was observed that the table does not list — update the table")
                .containsAll(covered);
    }
}
