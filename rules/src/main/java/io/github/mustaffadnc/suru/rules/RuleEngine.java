package io.github.mustaffadnc.suru.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runs every applicable rule against one observation.
 *
 * <p>Holds the rule set, not the state: states go through an {@link AlertStateStore} so the same
 * engine works over a Kafka Streams store, a map in a test, or anything else that can remember a
 * value by key. The engine itself is immutable and safe to share.
 */
public final class RuleEngine {

    private final List<Rule> rules;

    /**
     * Creates an engine over a fixed rule set.
     *
     * @param rules the rules to evaluate
     */
    public RuleEngine(List<Rule> rules) {
        this.rules = List.copyOf(Objects.requireNonNull(rules, "rules"));
    }

    /**
     * The rules this engine evaluates.
     *
     * @return the rule set
     */
    public List<Rule> rules() {
        return rules;
    }

    /**
     * Evaluates every rule that applies to the observed device, updating the store.
     *
     * <p>Rules are independent: one device can hold an open geofence alert and a pending battery
     * alert at the same time, and neither affects the other's debounce.
     *
     * @param observation the device's state
     * @param store where alert states are kept
     * @return alerts opened or closed by this observation, usually empty
     */
    public List<Alert> observe(Observation observation, AlertStateStore store) {
        List<Alert> alerts = new ArrayList<>();
        for (Rule rule : rules) {
            if (!rule.appliesTo(observation)) {
                continue;
            }
            String key = rule.alertKey(observation.deviceId());
            AlertState previous = store.get(key);
            if (previous == null) {
                previous = AlertState.initial(observation.at());
            }

            RuleEvaluator.Evaluation evaluation = RuleEvaluator.step(rule, previous, observation);
            if (!evaluation.state().equals(previous)) {
                store.put(key, evaluation.state());
            }
            evaluation.alert().ifPresent(alerts::add);
        }
        return alerts;
    }
}
