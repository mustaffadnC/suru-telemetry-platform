package io.github.mustaffadnc.suru.controlplane.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Every request carries a verified token, or it does not get in.
 *
 * <p>The resource server checks the signature, issuer and expiry before any application code runs,
 * so {@link PrincipalResolver} works with claims that have already been proven rather than with
 * headers a caller wrote themselves.
 *
 * <h2>Deny by default</h2>
 *
 * <p>{@code anyRequest().authenticated()} comes last and covers everything not explicitly opened
 * above it. The alternative — listing what needs protecting — means the next endpoint someone adds
 * is public until they remember, and nothing fails to remind them.
 *
 * <p>CSRF protection is disabled, which is correct here and would not be in a cookie-authenticated
 * application: the credential is a bearer token that a browser will not attach on its own, so there
 * is no cross-site request to forge. Sessions are stateless for the same reason.
 */
@Configuration
public class SecurityConfig {

    /**
     * Builds the filter chain.
     *
     * @param http the builder
     * @return the chain
     * @throws Exception if the chain cannot be built
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        return http.csrf(csrf -> csrf.disable())
                .sessionManagement(
                        session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(
                        requests ->
                                requests
                                        // Liveness and the API description carry nothing worth
                                        // protecting, and an unauthenticated probe is the point.
                                        .requestMatchers(
                                                HttpMethod.GET,
                                                "/actuator/health",
                                                "/actuator/health/**",
                                                "/v3/api-docs",
                                                "/v3/api-docs/**",
                                                "/swagger-ui.html",
                                                "/swagger-ui/**")
                                        .permitAll()
                                        .anyRequest()
                                        .authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()))
                .build();
    }
}
