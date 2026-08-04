package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The rolling windows one device keeps, one per metric any rule needs a trend for.
 *
 * <p>A record rather than a bare map so it carries its own type through a JSON serde: a
 * {@code Map<String, RollingWindow>} deserialises to a map of maps without one.
 *
 * @param windows base metric name to its window
 */
public record DeviceWindows(Map<String, RollingWindow> windows) {

    /** Copies the map so a stored value cannot change under a reader. */
    public DeviceWindows {
        windows = Map.copyOf(Objects.requireNonNull(windows, "windows"));
    }

    /**
     * An empty set of windows.
     *
     * @return the value
     */
    public static DeviceWindows empty() {
        return new DeviceWindows(Map.of());
    }

    /**
     * This value with one sample recorded against a metric's window.
     *
     * @param metric the base metric
     * @param at when the sample was taken
     * @param value the reading
     * @param span how far back the window reaches
     * @param capacity the most samples it holds
     * @return a new value
     */
    public DeviceWindows with(
            String metric, Instant at, double value, Duration span, int capacity) {
        RollingWindow window = windows.get(metric);
        if (window == null) {
            window = RollingWindow.of(span, capacity);
        }
        Map<String, RollingWindow> next = new HashMap<>(windows);
        next.put(metric, window.with(at, value));
        return new DeviceWindows(next);
    }

    /**
     * Every windowed statistic, under its derived metric name.
     *
     * @param minimumSamples how many samples a slope needs before it is published
     * @return derived metric names mapped to their values
     */
    public Map<String, Double> derivedMetrics(int minimumSamples) {
        Map<String, Double> derived = new HashMap<>();
        windows.forEach(
                (metric, window) ->
                        derived.putAll(
                                DerivedMetrics.valuesOf(metric, window.stats(), minimumSamples)));
        return derived;
    }
}
