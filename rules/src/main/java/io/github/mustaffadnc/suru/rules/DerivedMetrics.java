package io.github.mustaffadnc.suru.rules;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Windowed statistics exposed as ordinary metric names.
 *
 * <p>A trend rule is written as a {@link Threshold} on a derived name —
 * {@code power.battery_remaining_pct#slope_per_min} below {@code -5} is "draining faster than 5 %
 * a minute". No new condition type, no change to {@link Observation}, {@link Condition} or the
 * state machine, and trend rules inherit hysteresis and debounce for free rather than
 * reimplementing them.
 *
 * <p>The alternative was a dedicated {@code Trend} condition carrying its own window. It would have
 * meant teaching {@code Observation} about history, widening its serialised form for every device
 * whether or not any rule needed a trend, and writing a second copy of the hysteresis logic. The
 * naming convention is the weaker part of this design and it buys all of that back.
 *
 * <p><b>Which metrics get a window is derived from the rules themselves</b>, by reading the base
 * name back out of any derived name a rule mentions. A trend rule therefore cannot be deployed
 * without its window: there is no second place to forget to configure.
 */
public final class DerivedMetrics {

    /** Separates a base metric from its statistic. Not {@code .}, which metric names already use. */
    public static final char SEPARATOR = '#';

    /** Least-squares rate of change, in metric units per minute. */
    public static final String SLOPE_PER_MIN = "slope_per_min";

    /** Arithmetic mean over the window. */
    public static final String MEAN = "mean";

    /** Population standard deviation over the window. */
    public static final String STDDEV = "stddev";

    /** Smallest value in the window. */
    public static final String MIN = "min";

    /** Largest value in the window. */
    public static final String MAX = "max";

    /** How many samples the window holds, so a rule can require enough to be meaningful. */
    public static final String COUNT = "count";

    private DerivedMetrics() {}

    /**
     * The derived name for a statistic of a metric.
     *
     * @param metric the base metric
     * @param statistic one of the constants on this class
     * @return the derived metric name
     */
    public static String name(String metric, String statistic) {
        return metric + SEPARATOR + statistic;
    }

    /**
     * The base metric a derived name refers to.
     *
     * @param name a metric name, derived or not
     * @return the base metric, or empty when the name is an ordinary metric
     */
    public static Optional<String> baseMetricOf(String name) {
        int at = name.indexOf(SEPARATOR);
        return at <= 0 ? Optional.empty() : Optional.of(name.substring(0, at));
    }

    /**
     * Every base metric the given rules need a window for.
     *
     * <p>Only {@link Threshold} conditions can name a derived metric; a geofence reads coordinates
     * and staleness reads the clock, so neither can express a trend.
     *
     * @param engine the rules to inspect
     * @return base metric names, in the order the rules mention them
     */
    public static Set<String> windowedMetricsOf(RuleEngine engine) {
        Set<String> metrics = new LinkedHashSet<>();
        for (Rule rule : engine.rules()) {
            if (rule.condition() instanceof Threshold threshold) {
                baseMetricOf(threshold.metric()).ifPresent(metrics::add);
            }
        }
        return metrics;
    }

    /**
     * How many samples a window needs before its slope is published at all.
     *
     * <p><b>This is the defence against a dropped reading firing a trend rule</b>, and it does the
     * job the estimator cannot. A least-squares fit is better than an endpoint difference but it is
     * not robust: an outlier near either end of the window has high leverage, and on four samples it
     * drags the slope almost as far as the cheap estimator would — or, mid-window, reverses its
     * sign. See {@link RollingWindow#stats()} for the measured figures.
     *
     * <p>Eight is chosen so a single bad sample is outnumbered seven to one. At 1 Hz telemetry that
     * is eight seconds before a trend rule can speak, which is nothing next to the windows these
     * rules run over.
     */
    public static final int DEFAULT_MINIMUM_SAMPLES = 8;

    /**
     * The derived values for one base metric.
     *
     * @param metric the base metric
     * @param stats the window summary
     * @param minimumSamples how many samples the slope requires; below this it is not published, so
     *     a rule reading it simply cannot fire rather than firing on noise
     * @return derived metric names mapped to their values
     */
    public static Map<String, Double> valuesOf(
            String metric, WindowStats stats, int minimumSamples) {
        Map<String, Double> values = new HashMap<>(8);
        values.put(name(metric, COUNT), (double) stats.count());
        if (stats.count() == 0) {
            return values;
        }
        values.put(name(metric, MEAN), stats.mean());
        values.put(name(metric, STDDEV), stats.standardDeviation());
        values.put(name(metric, MIN), stats.min());
        values.put(name(metric, MAX), stats.max());

        // Absent rather than zero. Publishing 0.0 from too few samples would read as a confidently
        // flat trend instead of an unknown one, and every Threshold treats a missing metric as
        // "does not hold" — which is exactly the wanted behaviour while the evidence is thin.
        if (stats.hasAtLeast(Math.max(2, minimumSamples))) {
            values.put(name(metric, SLOPE_PER_MIN), stats.slopePerMinute());
        }
        return values;
    }
}
