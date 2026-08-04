package io.github.mustaffadnc.suru.rules;

/**
 * A test applied to a device's current state.
 *
 * <p><b>{@code active} is what makes hysteresis possible.</b> A condition is asked whether it holds
 * <em>given whether its alert is already firing</em>, so it can answer with a different threshold in
 * each direction: a battery rule fires below 20 % and only stops holding above 25 %. Without that
 * argument a value hovering on a single threshold produces an alert that fires and clears on every
 * sample, and the operator learns to ignore it — which is worse than not having the rule.
 *
 * <p>Time-based debouncing is deliberately <em>not</em> here. This interface answers "is it true
 * now"; {@link RuleEvaluator} answers "has it been true long enough to be worth telling someone".
 * Keeping the two apart is what lets either be tested without the other.
 */
public sealed interface Condition permits Threshold, Geofence, Staleness {

    /**
     * Whether this condition currently holds.
     *
     * @param observation the device's state
     * @param active whether the alert for this rule is currently firing, which selects the
     *     hysteresis band
     * @return {@code true} when the condition is met
     */
    boolean holds(Observation observation, boolean active);

    /**
     * A short description for the alert message.
     *
     * @param observation the device's state
     * @return human-readable summary of what was observed
     */
    String describe(Observation observation);
}
