package io.github.mustaffadnc.suru.controlplane.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mustaffadnc.suru.storage.TelemetryCopyWriter;
import io.github.mustaffadnc.suru.storage.TelemetryRow;
import io.github.mustaffadnc.suru.storage.TelemetrySchema;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@org.springframework.context.annotation.Import(
        io.github.mustaffadnc.suru.controlplane.security.TestTokens.class)
class TelemetryApiIT {

    private static final String TENANT = "tenant-a";
    private static final String DEVICE = "link-1/sys1";
    private static final String METRIC = "power.battery_v";

    /** Where the seeded data ends; every query is expressed relative to this. */
    private static final Instant SERIES_END = Instant.now().truncatedTo(ChronoUnit.HOURS);

    /** Six hours of one-second samples, which is enough to exercise all three resolutions. */
    private static final Instant SERIES_START = SERIES_END.minus(Duration.ofHours(6));

    private static PostgreSQLContainer container;

    @Autowired private WebApplicationContext context;

    private MockMvc mvc;

    @BeforeAll
    static void startDatabase() throws SQLException {
        container =
                new PostgreSQLContainer(
                                DockerImageName.parse("timescale/timescaledb:latest-pg17")
                                        .asCompatibleSubstituteFor("postgres"))
                        .withDatabaseName("suru")
                        .withUsername("suru")
                        .withPassword("suru_test_only")
                        .withCommand("postgres", "-c", "shared_preload_libraries=timescaledb");
        container.start();

        DataSource dataSource = dataSource();
        TelemetrySchema.migrate(dataSource);
        seed(dataSource);
    }

    private static DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(4);
        return new HikariDataSource(config);
    }

    /** Writes six hours of one-second battery samples plus a second metric and a second device. */
    private static void seed(DataSource dataSource) throws SQLException {
        TelemetryCopyWriter writer = new TelemetryCopyWriter(dataSource);
        List<TelemetryRow> rows = new ArrayList<>();
        long seconds = Duration.between(SERIES_START, SERIES_END).toSeconds();

        for (long s = 0; s < seconds; s++) {
            Instant at = SERIES_START.plusSeconds(s);
            // A slow discharge with a deterministic ripple, so min and max differ from the mean
            // within every bucket and an aggregation error cannot hide.
            double battery = 12.6 - (s / (double) seconds) * 1.4 + ((s % 10) - 5) * 0.01;
            rows.add(new TelemetryRow(at, TENANT, DEVICE, METRIC, battery));
            if (s % 60 == 0) {
                rows.add(new TelemetryRow(at, TENANT, DEVICE, "gps.satellites", 10 + (s % 3)));
            }
        }
        rows.add(new TelemetryRow(SERIES_END.minusSeconds(30), TENANT, "link-2/sys1", METRIC, 11.1));
        // Another tenant's data, to prove queries do not cross the boundary.
        rows.add(new TelemetryRow(SERIES_END.minusSeconds(30), "tenant-b", DEVICE, METRIC, 1.23));

        writer.write(rows);

        // Refresh the rollups rather than relying on their schedule. TimescaleDB's real-time
        // aggregation would union un-materialised buckets with a live read of the raw table, so
        // a query would return something either way — but what it returned would depend on when
        // the background job last ran, which is not a property a test should assert against.
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CALL refresh_continuous_aggregate('telemetry_1m', NULL, NULL)");
            statement.execute("CALL refresh_continuous_aggregate('telemetry_1h', NULL, NULL)");
        }
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", container::getJdbcUrl);
        registry.add("spring.datasource.username", container::getUsername);
        registry.add("spring.datasource.password", container::getPassword);
    }

    private MockMvc mvc() {
        if (mvc == null) {
            mvc =
                    MockMvcBuilders.webAppContextSetup(context)
                            .apply(
                                    org.springframework.security.test.web.servlet.setup
                                            .SecurityMockMvcConfigurers.springSecurity())
                            .build();
        }
        return mvc;
    }

    /** A signed OBSERVER token for a tenant; reading needs nothing more. */
    private static String bearer(String tenant) {
        return "Bearer "
                + io.github.mustaffadnc.suru.controlplane.security.TestTokens.token(
                        tenant, "reader@" + tenant, "OBSERVER");
    }

    @Test
    @DisplayName("A six-hour query at 100 points is answered from the minute rollup")
    void longRangeUsesRollup() throws Exception {
        // Six hours over 100 points is a 216-second bucket — above a minute, so the minute
        // rollup has the detail. At 500 points the bucket would be 43 seconds and the same
        // range would correctly drop to raw; the budget decides, not the span alone.
        mvc().perform(
                        get("/api/v1/telemetry")
                                .header("Authorization", bearer(TENANT))
                                .param("device", DEVICE)
                                .param("metric", METRIC)
                                .param("from", SERIES_START.toString())
                                .param("to", SERIES_END.toString())
                                .param("maxPoints", "100"))
                .andExpect(status().isOk())
                // The response says which source answered, because a spike that survives raw
                // and vanishes at rollup resolution is a different fact about the aircraft
                // than a spike that was never there.
                .andExpect(jsonPath("$.resolution").value("MINUTE"))
                .andExpect(jsonPath("$.deviceId").value(DEVICE))
                .andExpect(jsonPath("$.points.length()").value(org.hamcrest.Matchers.lessThanOrEqualTo(500)))
                .andExpect(jsonPath("$.points.length()").value(org.hamcrest.Matchers.greaterThan(0)));
    }

    @Test
    @DisplayName("A two-minute query drops to raw samples")
    void shortRangeUsesRaw() throws Exception {
        mvc().perform(
                        get("/api/v1/telemetry")
                                .header("Authorization", bearer(TENANT))
                                .param("device", DEVICE)
                                .param("metric", METRIC)
                                .param("from", SERIES_END.minus(Duration.ofMinutes(2)).toString())
                                .param("to", SERIES_END.toString())
                                .param("maxPoints", "500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resolution").value("RAW"))
                .andExpect(jsonPath("$.points[0].samples").value(1));
    }

    @Test
    @DisplayName("Bucket minimum and maximum bracket its mean")
    void aggregatesAreConsistent() throws Exception {
        String body =
                mvc().perform(
                                get("/api/v1/telemetry")
                                        .header("Authorization", bearer(TENANT))
                                        .param("device", DEVICE)
                                        .param("metric", METRIC)
                                        .param("from", SERIES_START.toString())
                                        .param("to", SERIES_END.toString())
                                        .param("maxPoints", "50"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse()
                        .getContentAsString();

        // Not a tautology: the mean is computed by weighting each rollup bucket by its sample
        // count, and a naive average of averages could drift outside the true min and max when
        // buckets hold different numbers of samples.
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var points = mapper.readTree(body).get("points");
        assertThat(points.size()).isPositive();
        for (var point : points) {
            double min = point.get("min").asDouble();
            double max = point.get("max").asDouble();
            double avg = point.get("avg").asDouble();
            assertThat(avg).isBetween(min, max);
            assertThat(point.get("samples").asLong()).isPositive();
        }
    }

    @Test
    @DisplayName("The latest reading of every metric comes back in one call")
    void latestPerMetric() throws Exception {
        mvc().perform(
                        get("/api/v1/devices/latest")
                                .header("Authorization", bearer(TENANT))
                                .param("device", DEVICE)
                                .param("windowHours", "24"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$['power.battery_v']").exists())
                .andExpect(jsonPath("$['gps.satellites']").exists());
    }

    @Test
    @DisplayName("A query never sees another tenant's rows")
    void tenantsAreIsolated() throws Exception {
        mvc().perform(get("/api/v1/devices").header("Authorization", bearer("tenant-b")).param("windowHours", "24"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));

        mvc().perform(get("/api/v1/devices").header("Authorization", bearer(TENANT)).param("windowHours", "24"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    @DisplayName("An inverted range is a client error, not a server error")
    void invertedRangeIsRejected() throws Exception {
        mvc().perform(
                        get("/api/v1/telemetry")
                                .header("Authorization", bearer(TENANT))
                                .param("device", DEVICE)
                                .param("metric", METRIC)
                                .param("from", SERIES_END.toString())
                                .param("to", SERIES_START.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    @DisplayName("A request with no token is refused, and one with no tenant claim too")
    void tenancyCannotBeOmitted() throws Exception {
        // This used to assert 400 for a missing X-Tenant-Id header. The header is gone: while
        // nothing was authenticated it was adequate, but once a token is required a header the
        // caller still controls is worse than no check at all, because an authenticated user of
        // one tenant could name another and be believed.
        mvc().perform(get("/api/v1/devices")).andExpect(status().isUnauthorized());

        // A properly signed token that says nothing about which tenant it belongs to. Defaulting
        // it would hand somebody's real data to a misconfigured client.
        String tenantless =
                io.github.mustaffadnc.suru.controlplane.security.TestTokens.token(
                        null, "reader@nowhere", "OBSERVER");
        mvc().perform(get("/api/v1/devices").header("Authorization", "Bearer " + tenantless))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("A token signed by an untrusted key cannot read telemetry")
    void forgedTokenCannotRead() throws Exception {
        String forged =
                io.github.mustaffadnc.suru.controlplane.security.TestTokens.forgedToken(
                        TENANT, "attacker@nowhere", "ADMIN");

        mvc().perform(get("/api/v1/devices").header("Authorization", "Bearer " + forged))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("The OpenAPI document is served")
    void openApiIsPublished() throws Exception {
        mvc().perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/v1/telemetry']").exists());
    }
}
