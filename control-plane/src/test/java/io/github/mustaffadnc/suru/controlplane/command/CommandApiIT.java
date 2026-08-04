package io.github.mustaffadnc.suru.controlplane.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mustaffadnc.suru.controlplane.security.TestTokens;
import io.github.mustaffadnc.suru.storage.TelemetrySchema;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The command API end to end against a real database.
 *
 * <p>Two of phase 5's acceptance criteria live here: an unauthorised request is refused <em>and
 * lands in the audit log</em>, and one tenant cannot see another's commands from any endpoint.
 */
@SpringBootTest
@org.springframework.context.annotation.Import(TestTokens.class)
class CommandApiIT {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("timescale/timescaledb:2.29.0-pg17")
                    .asCompatibleSubstituteFor("postgres");

    private static final PostgreSQLContainer CONTAINER =
            new PostgreSQLContainer(IMAGE)
                    .withDatabaseName("suru")
                    .withUsername("suru")
                    .withPassword("suru_test_only")
                    .withCommand("postgres", "-c", "shared_preload_libraries=timescaledb");

    private static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    static void startContainer() {
        CONTAINER.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", CONTAINER::getUsername);
        registry.add("spring.datasource.password", CONTAINER::getPassword);
    }

    @Autowired private WebApplicationContext context;

    @Autowired private DataSource dataSource;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        // The real filter chain, so a request without a valid token is rejected by the same code
        // that rejects one in production.
        mockMvc =
                MockMvcBuilders.webAppContextSetup(context)
                        .apply(
                                org.springframework.security.test.web.servlet.setup
                                        .SecurityMockMvcConfigurers.springSecurity())
                        .build();
        TelemetrySchema.migrate(dataSource);
        execute(
                """
                INSERT INTO tenant (tenant_id, display_name) VALUES ('acme', 'Acme')
                ON CONFLICT DO NOTHING
                """);
        execute(
                """
                INSERT INTO tenant (tenant_id, display_name) VALUES ('rival', 'Rival')
                ON CONFLICT DO NOTHING
                """);
    }

    private void execute(String sql) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("failed: " + sql, e);
        }
    }

    private long deniedRowsFor(String actor) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                "SELECT count(*) FROM audit_log WHERE outcome = 'DENIED'"
                                        + " AND actor = '%s'".formatted(actor))) {
            rows.next();
            return rows.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Presents a signed token, or none at all when the tenant is null. */
    private static MockHttpServletRequestBuilder identify(
            MockHttpServletRequestBuilder builder, String tenant, String actor, String roles) {
        if (tenant == null && actor == null) {
            return builder;
        }
        String[] roleNames = roles == null ? new String[0] : roles.split(",");
        return builder.header("Authorization", "Bearer " + TestTokens.token(tenant, actor, roleNames));
    }

    /** Presents a raw token value. */
    private static MockHttpServletRequestBuilder bearer(
            MockHttpServletRequestBuilder builder, String token) {
        return builder.header("Authorization", "Bearer " + token);
    }

    private MvcResult issue(
            String tenant, String actor, String roles, String device, CommandType type, String key)
            throws Exception {
        String body =
                """
                {"deviceId":"%s","type":"%s","params":{},"idempotencyKey":"%s"}"""
                        .formatted(device, type.name(), key);
        return mockMvc.perform(
                        identify(post("/api/v1/commands"), tenant, actor, roles)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(body))
                .andReturn();
    }

    private static JsonNode json(MvcResult result) throws Exception {
        return JSON.readTree(result.getResponse().getContentAsString());
    }

    @Test
    @DisplayName("an operator can issue a command and read it back")
    void operatorCanIssue() throws Exception {
        MvcResult created =
                issue("acme", "operator@acme", "OPERATOR", "link/api1", CommandType.ARM, "api-k1");

        assertThat(created.getResponse().getStatus()).isEqualTo(201);
        JsonNode body = json(created);
        assertThat(body.get("state").asText()).isEqualTo("PENDING");
        assertThat(body.get("issuedBy").asText()).isEqualTo("operator@acme");

        MvcResult read =
                mockMvc.perform(
                                identify(
                                        get("/api/v1/commands/" + body.get("id").asText()),
                                        "acme",
                                        "operator@acme",
                                        "OPERATOR"))
                        .andReturn();

        assertThat(read.getResponse().getStatus()).isEqualTo(200);
        assertThat(json(read).get("id").asText()).isEqualTo(body.get("id").asText());
    }

    /**
     * The acceptance criterion: a refused request is refused <em>and recorded</em>.
     *
     * <p>A denial nobody can find afterwards is exactly the one an incident review needs.
     */
    @Test
    @DisplayName("an observer is refused, and the refusal reaches the audit log")
    void observerIsRefusedAndAudited() throws Exception {
        long before = deniedRowsFor("watcher@acme");

        MvcResult response =
                issue("acme", "watcher@acme", "OBSERVER", "link/api2", CommandType.ARM, "api-k2");

        assertThat(response.getResponse().getStatus()).isEqualTo(403);
        assertThat(deniedRowsFor("watcher@acme"))
                .as("the refusal is durable, not just a status code")
                .isEqualTo(before + 1);
    }

    @Test
    @DisplayName("an unparseable role grants nothing rather than defaulting")
    void unknownRoleGrantsNothing() throws Exception {
        MvcResult response =
                issue("acme", "typo@acme", "OPERATOl", "link/api3", CommandType.ARM, "api-k3");

        assertThat(response.getResponse().getStatus())
                .as("a typo becoming an escalation is how this goes wrong")
                .isEqualTo(403);
    }

    @Test
    @DisplayName("a request with no token is refused before anything else happens")
    void anonymousIsUnauthenticated() throws Exception {
        MvcResult response =
                issue(null, null, "ADMIN", "link/api4", CommandType.ARM, "api-k4");

        assertThat(response.getResponse().getStatus()).isEqualTo(401);
    }

    private MvcResult issueWithToken(String token, String device, String key) throws Exception {
        String body =
                """
                {"deviceId":"%s","type":"ARM","params":{},"idempotencyKey":"%s"}"""
                        .formatted(device, key);
        return mockMvc.perform(
                        bearer(post("/api/v1/commands"), token)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(body))
                .andReturn();
    }

    /**
     * The test that separates reading claims from checking who wrote them.
     *
     * <p>The token is well formed and claims every role there is. It is signed by a key the
     * application does not trust, which is the only thing wrong with it — and the only thing that
     * needs to be wrong. An implementation that decoded the claims without verifying the signature
     * would pass every other test in this class.
     */
    @Test
    @DisplayName("a token signed by an untrusted key is rejected however good its claims look")
    void forgedTokenIsRejected() throws Exception {
        String forged = TestTokens.forgedToken("acme", "attacker@nowhere", "ADMIN", "OPERATOR");

        MvcResult response = issueWithToken(forged, "link/forged", "api-forged");

        assertThat(response.getResponse().getStatus()).isEqualTo(401);
    }

    @Test
    @DisplayName("an expired token is rejected even though it was signed correctly")
    void expiredTokenIsRejected() throws Exception {
        String expired = TestTokens.expiredToken("acme", "operator@acme", "OPERATOR");

        MvcResult response = issueWithToken(expired, "link/expired", "api-expired");

        assertThat(response.getResponse().getStatus()).isEqualTo(401);
    }

    /**
     * A valid token that says nothing about which tenant it belongs to.
     *
     * <p>403 rather than 401, and rather than a default: the caller authenticated successfully, so
     * re-authenticating will not help, and any default tenant would be somebody's real data.
     */
    @Test
    @DisplayName("a valid token with no tenant claim is refused rather than defaulted")
    void tokenWithoutTenantIsRefused() throws Exception {
        String tenantless = TestTokens.token(null, "operator@nowhere", "OPERATOR");

        MvcResult response = issueWithToken(tenantless, "link/notenant", "api-notenant");

        assertThat(response.getResponse().getStatus()).isEqualTo(403);
    }

    /**
     * The other acceptance criterion: no endpoint leaks across tenants.
     *
     * <p>{@code 404} rather than {@code 403} for the read: distinguishing them would confirm the id
     * exists somewhere. The controller never learns the difference either, because the lookup is
     * tenant-scoped rather than checked after the fact.
     */
    @Test
    @DisplayName("one tenant cannot see another's command from any endpoint")
    void tenantsAreIsolated() throws Exception {
        MvcResult mine =
                issue(
                        "acme", "operator@acme", "OPERATOR", "link/secret", CommandType.TAKEOFF,
                        "api-secret");
        assertThat(mine.getResponse().getStatus()).isEqualTo(201);
        String id = json(mine).get("id").asText();

        MvcResult byId =
                mockMvc.perform(
                                identify(
                                        get("/api/v1/commands/" + id),
                                        "rival",
                                        "operator@rival",
                                        "OPERATOR"))
                        .andReturn();
        assertThat(byId.getResponse().getStatus()).isEqualTo(404);

        MvcResult byDevice =
                mockMvc.perform(
                                identify(
                                        get("/api/v1/commands").param("deviceId", "link/secret"),
                                        "rival",
                                        "operator@rival",
                                        "OPERATOR"))
                        .andReturn();
        assertThat(byDevice.getResponse().getStatus()).isEqualTo(200);
        assertThat(byDevice.getResponse().getContentAsString())
                .as("knowing the device id is not enough to read another tenant's commands")
                .isEqualTo("[]");
    }

    @Test
    @DisplayName("the tenant comes from the caller's identity, not from anything they can name")
    void tenantCannotBeChosenByTheCaller() throws Exception {
        MvcResult created =
                issue(
                        "rival", "operator@rival", "OPERATOR", "link/api5", CommandType.LAND,
                        "api-k5");
        assertThat(created.getResponse().getStatus()).isEqualTo(201);
        String id = json(created).get("id").asText();

        MvcResult fromAcme =
                mockMvc.perform(
                                identify(
                                        get("/api/v1/commands/" + id),
                                        "acme",
                                        "operator@acme",
                                        "OPERATOR"))
                        .andReturn();

        assertThat(fromAcme.getResponse().getStatus()).isEqualTo(404);
    }

    @Test
    @DisplayName("a repeated idempotency key returns 200 with the original command")
    void idempotentReissueOverHttp() throws Exception {
        MvcResult first =
                issue("acme", "operator@acme", "OPERATOR", "link/api6", CommandType.ARM, "api-k6");
        MvcResult second =
                issue("acme", "operator@acme", "OPERATOR", "link/api6", CommandType.ARM, "api-k6");

        assertThat(first.getResponse().getStatus()).isEqualTo(201);
        assertThat(second.getResponse().getStatus())
                .as("200 rather than 201: nothing new was created")
                .isEqualTo(200);
        assertThat(json(second).get("id").asText()).isEqualTo(json(first).get("id").asText());
    }
}
