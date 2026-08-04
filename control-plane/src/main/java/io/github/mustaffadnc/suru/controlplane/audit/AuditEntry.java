package io.github.mustaffadnc.suru.controlplane.audit;

import java.util.Map;
import java.util.Objects;

/**
 * One line in the audit log: who did what to which thing, and whether it was allowed.
 *
 * @param tenantId the tenant the action was scoped to
 * @param actor who asked
 * @param action what they asked for, e.g. {@code command.issue}
 * @param subject what it was about, e.g. a device id, or {@code null}
 * @param outcome whether it was permitted
 * @param detail anything else worth keeping, such as the command parameters
 */
public record AuditEntry(
        String tenantId,
        String actor,
        String action,
        String subject,
        Outcome outcome,
        Map<String, String> detail) {

    /** Whether the action went ahead. */
    public enum Outcome {
        /** Permitted and carried out. */
        ALLOWED,
        /** Refused. This is the row an auditor comes looking for. */
        DENIED,
        /** Permitted, then failed for a reason unrelated to permission. */
        FAILED
    }

    /** Copies the detail map so an entry cannot change after it is recorded. */
    public AuditEntry {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(actor, "actor");
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(outcome, "outcome");
        detail = Map.copyOf(detail);
    }

    /**
     * An allowed action.
     *
     * @param tenantId the tenant
     * @param actor who asked
     * @param action what they asked for
     * @param subject what it was about
     * @param detail anything else worth keeping
     * @return the entry
     */
    public static AuditEntry allowed(
            String tenantId,
            String actor,
            String action,
            String subject,
            Map<String, String> detail) {
        return new AuditEntry(tenantId, actor, action, subject, Outcome.ALLOWED, detail);
    }

    /**
     * A refused action.
     *
     * @param tenantId the tenant
     * @param actor who asked
     * @param action what they asked for
     * @param subject what it was about
     * @param reason why it was refused
     * @return the entry
     */
    public static AuditEntry denied(
            String tenantId, String actor, String action, String subject, String reason) {
        return new AuditEntry(
                tenantId, actor, action, subject, Outcome.DENIED, Map.of("reason", reason));
    }
}
