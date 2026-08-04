package io.github.mustaffadnc.suru.controlplane.security;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

/**
 * Works out who is calling, from a token that has already been verified.
 *
 * <p>The resource server checks the signature, issuer and expiry before this runs, so the claims
 * read here are ones the identity provider asserted rather than ones the caller typed. Everything
 * downstream takes its tenant and its roles from the resulting {@link Principal} and never from a
 * path variable, query parameter or body field — which is what stops a later endpoint quietly
 * reading across tenants.
 *
 * <h2>Where the claims come from</h2>
 *
 * <p>{@code tenant} and {@code sub} are read directly. Roles are looked for in Keycloak's two
 * usual places — {@code realm_access.roles} and {@code resource_access.*.roles} — and in a plain
 * top-level {@code roles} claim, because which one a deployment uses depends on whether the roles
 * were assigned at the realm or at the client.
 *
 * <p><b>A token with no tenant claim is refused rather than defaulted.</b> There is no sensible
 * default: any choice would grant access to somebody's data, and the safe-looking option of
 * falling back to a "default" tenant is how a misconfigured client ends up reading a real one.
 */
@Component
public final class PrincipalResolver {

    /** Claim carrying the tenant this token is scoped to. */
    public static final String CLAIM_TENANT = "tenant";

    /** Top-level claim carrying a role list, for providers that do not nest them. */
    public static final String CLAIM_ROLES = "roles";

    /** Keycloak's realm-level role claim. */
    public static final String CLAIM_REALM_ACCESS = "realm_access";

    /** Keycloak's client-level role claim. */
    public static final String CLAIM_RESOURCE_ACCESS = "resource_access";

    /**
     * Resolves the caller from the current security context.
     *
     * @return the principal
     * @throws UnauthenticatedException when there is no verified token
     * @throws MissingTenantException when the token carries no tenant
     */
    public Principal resolve() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !(authentication.getPrincipal() instanceof Jwt jwt)) {
            throw new UnauthenticatedException("no verified token on the request");
        }
        return resolve(jwt);
    }

    /**
     * Resolves the caller from a verified token.
     *
     * @param jwt the token
     * @return the principal
     * @throws MissingTenantException when the token carries no tenant
     */
    public Principal resolve(Jwt jwt) {
        String tenant = jwt.getClaimAsString(CLAIM_TENANT);
        if (tenant == null || tenant.isBlank()) {
            throw new MissingTenantException("token carries no tenant claim");
        }
        String subject = jwt.getSubject();
        if (subject == null || subject.isBlank()) {
            throw new MissingTenantException("token carries no subject");
        }
        return new Principal(tenant.trim(), subject, rolesOf(jwt));
    }

    /**
     * Collects role names from every claim shape this platform accepts.
     *
     * <p><b>An unrecognised role name is dropped, not defaulted.</b> Mapping an unknown role to
     * anything but nothing is how a typo becomes an escalation, and a token whose roles all fail to
     * parse is left with none and will be refused.
     */
    private static Set<Role> rolesOf(Jwt jwt) {
        List<String> names = new ArrayList<>();
        addAll(names, jwt.getClaim(CLAIM_ROLES));

        Object realmAccess = jwt.getClaim(CLAIM_REALM_ACCESS);
        if (realmAccess instanceof Map<?, ?> realm) {
            addAll(names, realm.get("roles"));
        }

        Object resourceAccess = jwt.getClaim(CLAIM_RESOURCE_ACCESS);
        if (resourceAccess instanceof Map<?, ?> resources) {
            for (Object client : resources.values()) {
                if (client instanceof Map<?, ?> clientRoles) {
                    addAll(names, clientRoles.get("roles"));
                }
            }
        }

        Set<Role> roles = EnumSet.noneOf(Role.class);
        for (String name : names) {
            for (Role role : Role.values()) {
                if (role.name().equalsIgnoreCase(name.trim())) {
                    roles.add(role);
                    break;
                }
            }
        }
        return roles;
    }

    private static void addAll(List<String> target, Object claim) {
        if (claim instanceof Collection<?> values) {
            for (Object value : values) {
                if (value != null) {
                    target.add(value.toString());
                }
            }
        }
    }

    /** The request carried no verified token. */
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

    /** The token was valid but says nothing about which tenant it belongs to. */
    public static final class MissingTenantException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message what was missing
         */
        public MissingTenantException(String message) {
            super(message);
        }
    }
}
