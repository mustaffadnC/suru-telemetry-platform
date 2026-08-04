package io.github.mustaffadnc.suru.controlplane.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Mints real signed tokens for the API tests.
 *
 * <p>Real signing rather than a mocked authentication object, so the tests exercise the resource
 * server's verification instead of stepping around it. A second, unrelated key pair is kept so a
 * test can present a properly-formed token signed by the wrong issuer — the case that separates
 * "we read the claims" from "we checked who wrote them".
 */
@TestConfiguration
public class TestTokens {

    private static final KeyPair TRUSTED = generate();
    private static final KeyPair STRANGER = generate();

    private static KeyPair generate() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The decoder the application uses under test, trusting only {@link #TRUSTED}.
     *
     * @return the decoder
     */
    @Bean
    public JwtDecoder jwtDecoder() {
        return NimbusJwtDecoder.withPublicKey((RSAPublicKey) TRUSTED.getPublic()).build();
    }

    /**
     * A valid token.
     *
     * @param tenant the tenant claim, or {@code null} to omit it
     * @param subject who the token is for
     * @param roles role names, placed in Keycloak's realm_access claim
     * @return the encoded token
     */
    public static String token(String tenant, String subject, String... roles) {
        return sign(TRUSTED, claims(tenant, subject, Instant.now().plusSeconds(300), roles));
    }

    /**
     * A well-formed token signed by a key the application does not trust.
     *
     * @param tenant the tenant claim
     * @param subject who the token claims to be for
     * @param roles role names
     * @return the encoded token
     */
    public static String forgedToken(String tenant, String subject, String... roles) {
        return sign(STRANGER, claims(tenant, subject, Instant.now().plusSeconds(300), roles));
    }

    /**
     * A correctly signed token that has already expired.
     *
     * @param tenant the tenant claim
     * @param subject who the token is for
     * @param roles role names
     * @return the encoded token
     */
    public static String expiredToken(String tenant, String subject, String... roles) {
        return sign(TRUSTED, claims(tenant, subject, Instant.now().minusSeconds(60), roles));
    }

    private static JWTClaimsSet claims(
            String tenant, String subject, Instant expiry, String... roles) {
        JWTClaimsSet.Builder builder =
                new JWTClaimsSet.Builder()
                        .subject(subject)
                        .issueTime(Date.from(Instant.now().minusSeconds(5)))
                        .expirationTime(Date.from(expiry))
                        .claim(
                                PrincipalResolver.CLAIM_REALM_ACCESS,
                                Map.of("roles", List.of(roles)));
        if (tenant != null) {
            builder.claim(PrincipalResolver.CLAIM_TENANT, tenant);
        }
        return builder.build();
    }

    private static String sign(KeyPair keyPair, JWTClaimsSet claims) {
        try {
            SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
            jwt.sign(new RSASSASigner((RSAPrivateKey) keyPair.getPrivate()));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }
}
