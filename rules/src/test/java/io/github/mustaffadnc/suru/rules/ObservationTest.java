package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Accumulating device state, and the ordering hazards in doing so. */
class ObservationTest {

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");

    @Test
    @DisplayName("metrics accumulate across samples")
    void metricsAccumulate() {
        Observation state =
                Observation.empty("acme", "link/sys1", T0)
                        .with("position.latitude_deg", 39.89, T0.plusSeconds(1))
                        .with("position.longitude_deg", 32.78, T0.plusSeconds(1))
                        .with("power.battery_remaining_pct", 74.0, T0.plusSeconds(2));

        assertThat(state.value("position.latitude_deg")).hasValue(39.89);
        assertThat(state.value("power.battery_remaining_pct")).hasValue(74.0);
        assertThat(state.lastSeen()).isEqualTo(T0.plusSeconds(2));
    }

    @Test
    @DisplayName("a late sample does not drag lastSeen backwards")
    void lastSeenIsMonotonic() {
        Observation state =
                Observation.empty("acme", "link/sys1", T0)
                        .with("power.battery_v", 12.4, T0.plusSeconds(30));

        Observation afterLateArrival = state.with("gps.satellites", 9.0, T0.plusSeconds(5));

        assertThat(afterLateArrival.lastSeen())
                .as("a record arriving out of order must not make a live device look silent")
                .isEqualTo(T0.plusSeconds(30));
        assertThat(afterLateArrival.value("gps.satellites"))
                .as("the value still lands — only the clock is protected")
                .hasValue(9.0);
    }

    @Test
    @DisplayName("asOf advances the evaluation time without touching lastSeen")
    void asOfAdvancesOnlyTheEvaluationTime() {
        Observation state = Observation.empty("acme", "link/sys1", T0);

        Observation later = state.asOf(T0.plusSeconds(45));

        assertThat(later.lastSeen()).isEqualTo(T0);
        assertThat(later.at()).isEqualTo(T0.plusSeconds(45));
        assertThat(later.silence()).isEqualTo(Duration.ofSeconds(45));
    }

    @Test
    @DisplayName("asOf never moves the clock backwards")
    void asOfIgnoresEarlierInstants() {
        Observation state = Observation.empty("acme", "link/sys1", T0).asOf(T0.plusSeconds(45));

        assertThat(state.asOf(T0.plusSeconds(10))).isSameAs(state);
    }

    @Test
    @DisplayName("the metric map is copied, so a caller's map cannot mutate an observation")
    void metricsAreCopied() {
        Map<String, Double> mutable = new HashMap<>();
        mutable.put("power.battery_v", 12.4);
        Observation state = new Observation("acme", "link/sys1", mutable, T0, T0);

        mutable.put("power.battery_v", 0.1);

        assertThat(state.value("power.battery_v"))
                .as("an observation being evaluated must not change under the rules")
                .hasValue(12.4);
    }
}
