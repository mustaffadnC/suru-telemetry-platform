package io.github.mustaffadnc.suru.controlplane.security;

import java.util.Objects;
import java.util.Set;

/**
 * Who is making a request, and on whose behalf.
 *
 * <p><b>The tenant lives here and nowhere else.</b> Every repository call takes it from this object
 * rather than from a request parameter, so a caller cannot name a tenant and be believed. That is
 * the difference between multi-tenancy that holds and multi-tenancy that holds until someone adds
 * an endpoint and forgets a check.
 *
 * @param tenantId the tenant this caller belongs to
 * @param subject who they are, as it will appear in the audit log
 * @param roles what they may do
 */
public record Principal(String tenantId, String subject, Set<Role> roles) {

    /** Copies the role set so a principal cannot gain a role after it is resolved. */
    public Principal {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(subject, "subject");
        roles = Set.copyOf(roles);
    }

    /**
     * Whether this caller may issue commands.
     *
     * @return {@code true} if any role permits it
     */
    public boolean mayCommand() {
        return roles.stream().anyMatch(Role::mayCommand);
    }

    /**
     * Whether this caller may read.
     *
     * @return {@code true} if any role permits it
     */
    public boolean mayRead() {
        return roles.stream().anyMatch(Role::mayRead);
    }
}
