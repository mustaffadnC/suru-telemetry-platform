package io.github.mustaffadnc.suru.rules;

import java.util.HashMap;
import java.util.Map;

/**
 * Where alert states live between evaluations.
 *
 * <p>An interface rather than a field, because the state has to outlive the process. Under Kafka
 * Streams this is a changelog-backed key-value store, so a restarted or rebalanced instance picks
 * up mid-debounce instead of starting every rule from {@link AlertPhase#INACTIVE} — which would
 * silently re-arm every pending alert and close every open one without telling anybody. In tests it
 * is a {@link HashMap}.
 */
public interface AlertStateStore {

    /**
     * Reads a state.
     *
     * @param key from {@link Rule#alertKey(String)}
     * @return the stored state, or {@code null} when this rule has not run for this device
     */
    AlertState get(String key);

    /**
     * Writes a state.
     *
     * @param key from {@link Rule#alertKey(String)}
     * @param state the state to keep
     */
    void put(String key, AlertState state);

    /**
     * An in-memory store.
     *
     * @return a store backed by a fresh map
     */
    static AlertStateStore inMemory() {
        Map<String, AlertState> map = new HashMap<>();
        return new AlertStateStore() {
            @Override
            public AlertState get(String key) {
                return map.get(key);
            }

            @Override
            public void put(String key, AlertState state) {
                map.put(key, state);
            }
        };
    }
}
