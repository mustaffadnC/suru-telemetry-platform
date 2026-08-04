package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The derived-metric naming convention and the minimum-sample gate. */
class DerivedMetricsTest {

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");
    private static final String BATTERY = "power.battery_remaining_pct";

    private static RollingWindow windowOf(int samples) {
        RollingWindow window = RollingWindow.of(Duration.ofMinutes(30), 64);
        for (int i = 0; i < samples; i++) {
            window = window.with(T0.plusSeconds(i * 60L), 90 - i);
        }
        return window;
    }

    @Test
    @DisplayName("a derived name round-trips back to its base metric")
    void namingRoundTrips() {
        String derived = DerivedMetrics.name(BATTERY, DerivedMetrics.SLOPE_PER_MIN);

        assertThat(derived).isEqualTo("power.battery_remaining_pct#slope_per_min");
        assertThat(DerivedMetrics.baseMetricOf(derived)).hasValue(BATTERY);
    }

    @Test
    @DisplayName("an ordinary metric name is not mistaken for a derived one")
    void plainMetricIsNotDerived() {
        assertThat(DerivedMetrics.baseMetricOf(BATTERY))
                .as("dots are ordinary in metric names, which is why the separator is not a dot")
                .isEmpty();
        assertThat(DerivedMetrics.baseMetricOf("position.latitude_deg")).isEmpty();
    }

    @Test
    @DisplayName("the slope is withheld until the window holds enough samples")
    void slopeIsGatedOnSampleCount() {
        String slope = DerivedMetrics.name(BATTERY, DerivedMetrics.SLOPE_PER_MIN);

        Map<String, Double> thin = DerivedMetrics.valuesOf(BATTERY, windowOf(4).stats(), 8);
        assertThat(thin)
                .as("a Threshold treats a missing metric as not holding, which is what is wanted")
                .doesNotContainKey(slope);
        assertThat(thin)
                .as("the other statistics are still published — only the slope needs the evidence")
                .containsKeys(
                        DerivedMetrics.name(BATTERY, DerivedMetrics.MEAN),
                        DerivedMetrics.name(BATTERY, DerivedMetrics.COUNT));

        Map<String, Double> enough = DerivedMetrics.valuesOf(BATTERY, windowOf(8).stats(), 8);
        assertThat(enough).containsKey(slope);
        assertThat(enough.get(slope)).isEqualTo(-1.0);
    }

    @Test
    @DisplayName("a slope is never published from a single sample, whatever the minimum says")
    void slopeNeedsTwoPointsRegardless() {
        Map<String, Double> single = DerivedMetrics.valuesOf(BATTERY, windowOf(1).stats(), 1);

        assertThat(single)
                .as("zero from one sample would read as a confidently flat trend")
                .doesNotContainKey(DerivedMetrics.name(BATTERY, DerivedMetrics.SLOPE_PER_MIN));
    }

    @Test
    @DisplayName("an empty window publishes only its count")
    void emptyWindow() {
        Map<String, Double> empty = DerivedMetrics.valuesOf(BATTERY, WindowStats.EMPTY, 8);

        assertThat(empty).containsOnlyKeys(DerivedMetrics.name(BATTERY, DerivedMetrics.COUNT));
        assertThat(empty.get(DerivedMetrics.name(BATTERY, DerivedMetrics.COUNT))).isZero();
    }

    @Test
    @DisplayName("windowed metrics are read out of the rules, not configured separately")
    void windowsFollowTheRules() {
        RuleEngine engine =
                new RuleEngine(
                        List.of(
                                rule("trend", DerivedMetrics.name(BATTERY, DerivedMetrics.SLOPE_PER_MIN)),
                                rule("level", BATTERY),
                                new Rule(
                                        "fence",
                                        "Fence",
                                        "acme",
                                        Rule.ALL_DEVICES,
                                        Geofence.around(39.0, 32.0, 1000),
                                        Duration.ZERO,
                                        Duration.ZERO,
                                        Severity.INFO)));

        assertThat(DerivedMetrics.windowedMetricsOf(engine))
                .as("only the trend rule needs a window; a level threshold and a fence do not")
                .containsExactly(BATTERY);
    }

    private static Rule rule(String id, String metric) {
        return new Rule(
                id,
                id,
                "acme",
                Rule.ALL_DEVICES,
                Threshold.at(metric, Threshold.Comparison.BELOW, -5.0),
                Duration.ZERO,
                Duration.ZERO,
                Severity.WARNING);
    }
}
