package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Something worth telling an operator: an alert opening or closing.
 *
 * <p>Three instants are carried rather than one, because "how fast did we detect it" is otherwise
 * unanswerable. {@code conditionSince} is when the fault began, {@code at} is when this alert was
 * produced, and their difference is the detection latency — but most of that difference is the
 * rule's own {@code forDuration}, which was chosen on purpose. {@link #incidentalLatency()}
 * subtracts it, leaving the part the platform is actually responsible for.
 *
 * <p>Reporting only the total is how a system gets credited or blamed for its own configuration: a
 * rule with a 30-second debounce would show a 30-second detection latency and look slow, while the
 * pipeline underneath it might have taken 4 ms.
 *
 * @param ruleId the rule that produced this
 * @param ruleName the rule's name, so a notification reads without a lookup
 * @param tenantId owning tenant
 * @param deviceId the device the alert is about
 * @param severity how urgent
 * @param kind whether this opens or closes the alert
 * @param conditionSince when the condition started holding, for a firing alert, or stopped holding
 *     for a resolving one
 * @param at when this alert was produced
 * @param debounce the rule's configured wait for this direction
 * @param detail what was observed, for the notification body
 */
public record Alert(
        String ruleId,
        String ruleName,
        String tenantId,
        String deviceId,
        Severity severity,
        Kind kind,
        Instant conditionSince,
        Instant at,
        Duration debounce,
        String detail) {

    /** Whether an alert is opening or closing. */
    public enum Kind {
        /** The condition held long enough; the alert is now open. */
        FIRED,
        /** The condition stopped holding long enough; the alert is closed. */
        RESOLVED
    }

    /** Validates the alert carries everything a notification needs. */
    public Alert {
        Objects.requireNonNull(ruleId, "ruleId");
        Objects.requireNonNull(ruleName, "ruleName");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(deviceId, "deviceId");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(conditionSince, "conditionSince");
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(debounce, "debounce");
        Objects.requireNonNull(detail, "detail");
    }

    /**
     * The deduplication key: one open alert per rule per device.
     *
     * @return the key
     */
    public String key() {
        return ruleId + '/' + deviceId;
    }

    /**
     * Time from the fault starting to this alert being produced, debounce included.
     *
     * @return the total latency
     */
    public Duration detectionLatency() {
        return Duration.between(conditionSince, at);
    }

    /**
     * Detection latency with the rule's configured debounce removed.
     *
     * <p>This is the number that says anything about the platform. It is normally a few
     * milliseconds and should stay there; if it grows, the pipeline is behind.
     *
     * @return the latency the platform is responsible for, never negative
     */
    public Duration incidentalLatency() {
        Duration incidental = detectionLatency().minus(debounce);
        return incidental.isNegative() ? Duration.ZERO : incidental;
    }

    @Override
    public String toString() {
        return "%s %s [%s] %s/%s — %s (%s after onset, %s of it debounce)"
                .formatted(
                        kind, severity, ruleName, tenantId, deviceId, detail,
                        detectionLatency(), debounce);
    }
}
