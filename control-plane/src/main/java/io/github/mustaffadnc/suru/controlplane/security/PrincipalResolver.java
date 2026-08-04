package io.github.mustaffadnc.suru.controlplane.security;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Works out who is calling.
 *
 * <h2>This is not authentication yet, and says so</h2>
 *
 * <p>Identity currently comes from request headers, which any caller can set. <b>There is no
 * security here at all</b> — a client can claim any tenant and any role simply by asking. What this
 * class does provide is the <em>shape</em>: every downstream call takes its tenant and its roles
 * from a {@link Principal}, never from a path variable, a query parameter or a body field. That is
 * the part that is hard to retrofit, and the part that keeps a later endpoint from quietly reading
 * across tenants.
 *
 * <p>Swapping headers for verified OIDC claims is a change to this one class: {@code tenant} and
 * {@code roles} become claims on a validated JWT and nothing downstream moves. Until then the
 * service must not be exposed to anything but a trusted network, and the tests that matter — tenant
 * isolation, denied requests reaching the audit log — are already meaningful, because they exercise
 * the enforcement rather than the identity.
 */
@Component
public final class PrincipalResolver {

    /** Header carrying the tenant. Replaced by a verified token claim. */
    public static final String HEADER_TENANT = "X-Tenant-Id";

    /** Header carrying the caller's identity. Replaced by the token subject. */
    public static final String HEADER_ACTOR = "X-Actor";

    /** Header carrying a comma-separated role list. Replaced by a token claim. */
    public static final String HEADER_ROLES = "X-Roles";

    /**
     * Resolves the caller.
     *
     * @param request the incoming request
     * @return the principal
     * @throws UnauthenticatedException when the request carries no usable identity
     */
    public Principal resolve(HttpServletRequest request) {
        String tenant = trimmed(request.getHeader(HEADER_TENANT));
        String actor = trimmed(request.getHeader(HEADER_ACTOR));
        if (tenant == null || actor == null) {
            throw new UnauthenticatedException("missing tenant or actor");
        }
        return new Principal(tenant, actor, parseRoles(request.getHeader(HEADER_ROLES)));
    }

    /**
     * Parses the role list.
     *
     * <p><b>An unrecognised role name is dropped, not defaulted.</b> Mapping an unknown role to
     * anything but nothing is how a typo becomes an escalation — {@code OPERATOR } with a trailing
     * space silently becoming an operator is exactly the bug. A caller whose roles all fail to
     * parse is left with none and will be refused.
     */
    private static Set<Role> parseRoles(String header) {
        Set<Role> roles = EnumSet.noneOf(Role.class);
        if (header == null || header.isBlank()) {
            return roles;
        }
        Arrays.stream(header.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .forEach(
                        name -> {
                            for (Role role : Role.values()) {
                                if (role.name().equalsIgnoreCase(name)) {
                                    roles.add(role);
                                    return;
                                }
                            }
                        });
        return roles;
    }

    private static String trimmed(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /** The request carried no usable identity. */
    public static final class UnauthenticatedException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message what was missing
         */
        public UnauthenticatedException(String message) {
            super(message);
        }
    }
}
