package io.github.mustaffadnc.suru.controlplane.security;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Turns a missing identity into a {@code 401}.
 *
 * <p>The body is empty on purpose. An unauthenticated caller has not established who they are, so
 * telling them which header was missing describes the platform's internals to someone who has yet
 * to prove they should be talking to it at all. The reason is in the server's logs, where the
 * operator can read it.
 */
@RestControllerAdvice
public final class SecurityExceptionHandler {

    /**
     * Handles a request with no usable identity.
     *
     * @param e the failure
     * @return {@code 401}
     */
    @ExceptionHandler(PrincipalResolver.UnauthenticatedException.class)
    public ResponseEntity<Void> unauthenticated(PrincipalResolver.UnauthenticatedException e) {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    }
}
