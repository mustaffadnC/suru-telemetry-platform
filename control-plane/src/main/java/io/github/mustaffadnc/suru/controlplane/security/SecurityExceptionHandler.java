package io.github.mustaffadnc.suru.controlplane.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Turns identity problems into status codes.
 *
 * <p>Bodies are empty on purpose. Telling a caller which claim was missing describes the platform's
 * configuration to someone who has not established that they should be talking to it; the reason
 * goes to the server's log, where an operator can read it.
 */
@RestControllerAdvice
public final class SecurityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(SecurityExceptionHandler.class);

    /**
     * No verified token.
     *
     * @param e the failure
     * @return {@code 401}
     */
    @ExceptionHandler(PrincipalResolver.UnauthenticatedException.class)
    public ResponseEntity<Void> unauthenticated(PrincipalResolver.UnauthenticatedException e) {
        log.debug("unauthenticated request: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    }

    /**
     * A valid token that says nothing about which tenant it belongs to.
     *
     * <p>{@code 403} rather than {@code 401}: the caller authenticated successfully, so retrying
     * with the same credential will not help. This is a misconfigured client, and telling it to
     * re-authenticate would send it round a loop.
     *
     * @param e the failure
     * @return {@code 403}
     */
    @ExceptionHandler(PrincipalResolver.MissingTenantException.class)
    public ResponseEntity<Void> missingTenant(PrincipalResolver.MissingTenantException e) {
        log.warn("token accepted but unusable: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }
}
