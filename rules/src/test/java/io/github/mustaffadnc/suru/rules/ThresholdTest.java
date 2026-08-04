package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.mustaffadnc.suru.rules.Threshold.Comparison;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Threshold comparison, hysteresis, and what a missing metric means. */
class ThresholdTest {

    private static final Instant NOW = Instant.parse("2026-08-04T12:00:00Z");
    private static final String METRIC = "power.battery_remaining_pct";

    private static Observation reading(double value) {
        return new Observation("acme", "link/sys1", Map.of(METRIC, value), NOW, NOW);
    }

    @Test
    @DisplayName("BELOW fires under the threshold and not on it")
    void belowIsStrict() {
        Threshold rule = Threshold.at(METRIC, Comparison.BELOW, 20.0);

        assertThat(rule.holds(reading(19.999), false)).isTrue();
        assertThat(rule.holds(reading(20.0), false)).isFalse();
        assertThat(rule.holds(reading(20.001), false)).isFalse();
    }

    @Test
    @DisplayName("ABOVE fires over the threshold and not on it")
    void aboveIsStrict() {
        Threshold rule = Threshold.at("motor.temperature_c", Comparison.ABOVE, 85.0);
        Observation hot =
                new Observation("acme", "link/sys1", Map.of("motor.temperature_c", 85.5), NOW, NOW);
        Observation exact =
                new Observation("acme", "link/sys1", Map.of("motor.temperature_c", 85.0), NOW, NOW);

        assertThat(rule.holds(hot, false)).isTrue();
        assertThat(rule.holds(exact, false)).isFalse();
    }

    @Test
    @DisplayName("the hysteresis band holds one answer between the two thresholds")
    void hysteresisBand() {
        Threshold rule = new Threshold(METRIC, Comparison.BELOW, 20.0, 25.0);

        // 22 is in the band: not bad enough to fire, not good enough to clear.
        assertThat(rule.holds(reading(22.0), false)).as("inactive: needs to drop below 20").isFalse();
        assertThat(rule.holds(reading(22.0), true)).as("active: needs to rise above 25").isTrue();

        assertThat(rule.holds(reading(26.0), true)).as("clear of the band entirely").isFalse();
        assertThat(rule.holds(reading(19.0), false)).as("into the band from below").isTrue();
    }

    @Test
    @DisplayName("a metric the device has never reported never fires")
    void missingMetricDoesNotFire() {
        Threshold rule = Threshold.at(METRIC, Comparison.BELOW, 20.0);
        Observation nothing = Observation.empty("acme", "link/sys1", NOW);

        assertThat(rule.holds(nothing, false))
                .as("absence is a job for Staleness, not for a threshold on a value")
                .isFalse();
        assertThat(rule.holds(nothing, true)).isFalse();
        assertThat(rule.describe(nothing)).contains("no reading");
    }

    @Test
    @DisplayName("an inverted hysteresis band is rejected in both directions")
    void invertedBandRejected() {
        assertThatThrownBy(() -> new Threshold(METRIC, Comparison.BELOW, 20.0, 15.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clearing must be the harder test");

        assertThatThrownBy(() -> new Threshold(METRIC, Comparison.ABOVE, 85.0, 90.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clearing must be the harder test");
    }

    @Test
    @DisplayName("a NaN threshold is rejected rather than silently never firing")
    void nonFiniteRejected() {
        assertThatThrownBy(() -> Threshold.at(METRIC, Comparison.BELOW, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finite");
    }
}
