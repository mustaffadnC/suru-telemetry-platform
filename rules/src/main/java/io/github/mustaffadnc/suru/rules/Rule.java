package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.util.Objects;

/**
 * A condition, who it applies to, and how long it has to hold before anyone is told.
 *
 * @param id stable identifier, used as the alert's deduplication key together with the device
 * @param name human-readable name, carried into the alert
 * @param tenantId the tenant this rule belongs to
 * @param deviceId the device it applies to, or {@link #ALL_DEVICES}
 * @param condition what is tested
 * @param forDuration how long the condition must hold continuously before the alert fires
 * @param clearDuration how long it must stop holding before the alert closes
 * @param severity how urgent the resulting alert is
 */
public record Rule(
        String id,
        String name,
        String tenantId,
        String deviceId,
        Condition condition,
        Duration forDuration,
        Duration clearDuration,
        Severity severity) {

    /** Device selector matching every device in the tenant. */
    public static final String ALL_DEVICES = "*";

    /** Rejects durations that would make the debounce meaningless or the alert unclosable. */
    public Rule {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(deviceId, "deviceId");
        Objects.requireNonNull(condition, "condition");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(forDuration, "forDuration");
        Objects.requireNonNull(clearDuration, "clearDuration");
        if (forDuration.isNegative()) {
            throw new IllegalArgumentException("forDuration must not be negative");
        }
        if (clearDuration.isNegative()) {
            throw new IllegalArgumentException("clearDuration must not be negative");
        }
    }

    /**
     * Whether this rule applies to the device an observation describes.
     *
     * @param observation the device's state
     * @return {@code true} when tenant matches and the device is selected
     */
    public boolean appliesTo(Observation observation) {
        return tenantId.equals(observation.tenantId())
                && (ALL_DEVICES.equals(deviceId) || deviceId.equals(observation.deviceId()));
    }

    /**
     * The key an alert for this rule and device is deduplicated under.
     *
     * @param deviceId the device the alert is about, which for a wildcard rule is not {@link
     *     #deviceId()}
     * @return the key
     */
    public String alertKey(String deviceId) {
        return id + '/' + deviceId;
    }
}
