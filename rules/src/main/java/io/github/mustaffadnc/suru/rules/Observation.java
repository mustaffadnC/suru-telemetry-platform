package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * What a device looks like at one instant: its latest value for every metric, and when it was last
 * heard from.
 *
 * <p><b>Rules evaluate against device state, not against individual samples.</b> A sample carries
 * one metric, and the conditions that matter most need more than one — a geofence needs latitude
 * and longitude together, which arrive as two separate measurements out of the same MAVLink
 * message. Evaluating per-sample would mean firing a geofence check against a latitude with no
 * longitude to pair it with.
 *
 * <p>{@code lastSeen} is separate from {@code at} on purpose, and the gap between them is the only
 * thing {@link Staleness} looks at. Every other condition reads {@code metrics}; that one reads the
 * silence.
 *
 * @param tenantId owning tenant
 * @param deviceId the device this describes
 * @param metrics latest value per metric name
 * @param lastSeen when this device last produced any telemetry
 * @param at the instant this evaluation is happening
 */
public record Observation(
        String tenantId,
        String deviceId,
        Map<String, Double> metrics,
        Instant lastSeen,
        Instant at) {

    /** Copies the metric map so an observation cannot change under a rule mid-evaluation. */
    public Observation {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(deviceId, "deviceId");
        Objects.requireNonNull(lastSeen, "lastSeen");
        Objects.requireNonNull(at, "at");
        metrics = Map.copyOf(metrics);
    }

    /**
     * The latest value of a metric.
     *
     * @param metric metric name
     * @return the value, or empty when this device has never reported it
     */
    public OptionalDouble value(String metric) {
        Double v = metrics.get(metric);
        return v == null ? OptionalDouble.empty() : OptionalDouble.of(v);
    }

    /**
     * How long this device has been silent.
     *
     * @return the gap between {@link #lastSeen()} and {@link #at()}
     */
    public Duration silence() {
        return Duration.between(lastSeen, at);
    }

    /**
     * An observation for a device nothing has been heard from yet.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param at the current instant, taken as both the evaluation time and the last-seen time
     * @return an empty observation
     */
    public static Observation empty(String tenantId, String deviceId, Instant at) {
        return new Observation(tenantId, deviceId, Map.of(), at, at);
    }

    /**
     * This observation with one metric updated and the device marked as heard from.
     *
     * <p><b>{@code lastSeen} never moves backwards.</b> Records can arrive out of order — different
     * partitions, a retry, a device catching up after a link drop — and a late sample that reset
     * {@code lastSeen} to its own older timestamp would make a device that is transmitting fine
     * look like it had gone quiet, firing a telemetry-loss alert on the strength of data arriving.
     *
     * @param metric metric name
     * @param value the new value
     * @param sampleTime when the sample was taken
     * @return a new observation
     */
    public Observation with(String metric, double value, Instant sampleTime) {
        Map<String, Double> updated = new HashMap<>(metrics);
        updated.put(metric, value);
        Instant seen = sampleTime.isAfter(lastSeen) ? sampleTime : lastSeen;
        Instant evaluatedAt = sampleTime.isAfter(at) ? sampleTime : at;
        return new Observation(tenantId, deviceId, updated, seen, evaluatedAt);
    }

    /**
     * This observation with several metrics updated at once.
     *
     * <p><b>An empty update still marks the device as heard from.</b> Most frames carry nothing
     * this platform stores — heartbeats above all — and a heartbeat is the clearest possible
     * evidence that a device is alive. Treating "produced no metrics" as "did not arrive" would
     * make {@link Staleness} fire on vehicles that are transmitting their liveness signal
     * perfectly.
     *
     * @param updates metric names and their new values
     * @param sampleTime when the sample was taken
     * @return a new observation
     */
    public Observation withAll(Map<String, Double> updates, Instant sampleTime) {
        Instant seen = sampleTime.isAfter(lastSeen) ? sampleTime : lastSeen;
        Instant evaluatedAt = sampleTime.isAfter(at) ? sampleTime : at;
        if (updates.isEmpty()) {
            return new Observation(tenantId, deviceId, metrics, seen, evaluatedAt);
        }
        Map<String, Double> merged = new HashMap<>(metrics);
        merged.putAll(updates);
        return new Observation(tenantId, deviceId, merged, seen, evaluatedAt);
    }

    /**
     * This observation re-evaluated at a later instant, with no new data.
     *
     * <p>This is how a timer asks "has anything changed by now" — the metrics stand, but the
     * silence has grown. It is the only way {@link Staleness} can ever hold.
     *
     * @param evaluatedAt the new evaluation time
     * @return a new observation, or this one when the instant is not later
     */
    public Observation asOf(Instant evaluatedAt) {
        return evaluatedAt.isAfter(at)
                ? new Observation(tenantId, deviceId, metrics, lastSeen, evaluatedAt)
                : this;
    }
}
