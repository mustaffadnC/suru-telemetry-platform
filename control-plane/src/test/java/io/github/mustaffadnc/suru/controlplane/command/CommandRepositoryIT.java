package io.github.mustaffadnc.suru.controlplane.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mustaffadnc.suru.storage.TelemetrySchema;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The command repository against a real PostgreSQL.
 *
 * <p>The test this class exists for is {@link #outboxFailureRollsBackTheCommand()}. Everything else
 * here would pass against an implementation that wrote the two rows in separate transactions, and
 * that implementation is broken in a way nobody notices until the day it matters.
 */
class CommandRepositoryIT {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("timescale/timescaledb:2.29.0-pg17")
                    .asCompatibleSubstituteFor("postgres");

    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";
    private static final String TOPIC = "commands.outbound";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static PostgreSQLContainer container;
    private static HikariDataSource dataSource;
    private static CommandRepository repository;

    @BeforeAll
    static void startDatabase() {
        container =
                new PostgreSQLContainer(IMAGE)
                        .withDatabaseName("suru")
                        .withUsername("suru")
                        .withPassword("suru_test_only")
                        .withCommand("postgres", "-c", "shared_preload_libraries=timescaledb");
        container.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(6);
        dataSource = new HikariDataSource(config);

        TelemetrySchema.migrate(dataSource);
        repository = new CommandRepository(dataSource);

        execute("INSERT INTO tenant (tenant_id, display_name) VALUES ('acme', 'Acme')");
        execute("INSERT INTO tenant (tenant_id, display_name) VALUES ('other', 'Other')");
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @BeforeEach
    void clearCommands() {
        execute("DELETE FROM command_outbox");
        execute("DELETE FROM command");
    }

    private static void execute(String sql) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("failed: " + sql, e);
        }
    }

    private static long count(String table) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
            rows.next();
            return rows.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private CommandRepository.Issued issue(String key, CommandType type) throws SQLException {
        return repository.issue(
                TENANT, DEVICE, key, type, Map.of(), "operator@acme", TOPIC, TIMEOUT);
    }

    @Test
    @DisplayName("issuing writes the command and its outbox row together")
    void issueWritesBoth() throws SQLException {
        CommandRepository.Issued issued = issue("k1", CommandType.ARM);

        assertThat(issued.created()).isTrue();
        assertThat(issued.command().state()).isEqualTo(CommandState.PENDING);
        assertThat(count("command")).isEqualTo(1);
        assertThat(count("command_outbox")).isEqualTo(1);
    }

    /**
     * The guarantee the outbox exists for.
     *
     * <p>A trigger makes the outbox insert fail after the command insert has already succeeded
     * inside the same transaction. If the two were separate transactions, the command row would
     * survive: an operator would see an accepted command that no relay will ever publish, and no
     * amount of retrying would fix it because the command already exists.
     */
    @Test
    @DisplayName("a failed outbox insert rolls the command back with it")
    void outboxFailureRollsBackTheCommand() {
        execute(
                """
                CREATE OR REPLACE FUNCTION fail_outbox() RETURNS TRIGGER AS $$
                BEGIN RAISE EXCEPTION 'outbox unavailable'; END;
                $$ LANGUAGE plpgsql
                """);
        execute(
                """
                CREATE TRIGGER outbox_breaks BEFORE INSERT ON command_outbox
                FOR EACH ROW EXECUTE FUNCTION fail_outbox()
                """);
        try {
            assertThatThrownBy(() -> issue("k-atomic", CommandType.ARM))
                    .isInstanceOf(SQLException.class)
                    .hasStackTraceContaining("outbox unavailable");

            assertThat(count("command"))
                    .as("a command nothing will ever publish is worse than no command")
                    .isZero();
            assertThat(count("command_outbox")).isZero();
        } finally {
            execute("DROP TRIGGER outbox_breaks ON command_outbox");
        }
    }

    @Test
    @DisplayName("a repeated idempotency key returns the original and issues nothing new")
    void idempotentReissue() throws SQLException {
        CommandRepository.Issued first = issue("same-key", CommandType.ARM);
        CommandRepository.Issued second = issue("same-key", CommandType.ARM);

        assertThat(second.created()).isFalse();
        assertThat(second.command().id()).isEqualTo(first.command().id());
        assertThat(count("command")).isEqualTo(1);
        assertThat(count("command_outbox"))
                .as("a second outbox row would send the vehicle a second ARM")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the same key under a different tenant is a different command")
    void idempotencyIsPerTenant() throws SQLException {
        issue("shared", CommandType.ARM);
        CommandRepository.Issued other =
                repository.issue(
                        "other", DEVICE, "shared", CommandType.ARM, Map.of(),
                        "operator@other", TOPIC, TIMEOUT);

        assertThat(other.created()).isTrue();
        assertThat(count("command")).isEqualTo(2);
    }

    @Test
    @DisplayName("an ACK of zero settles the command as accepted")
    void ackSettlesTheCommand() throws SQLException {
        UUID id = issue("k-ack", CommandType.TAKEOFF).command().id();

        assertThat(repository.recordAck(id, 0, Instant.now())).isTrue();

        Command settled = repository.find(TENANT, id).orElseThrow();
        assertThat(settled.state()).isEqualTo(CommandState.ACKED);
        assertThat(settled.ack()).hasValue(0);
        assertThat(settled.awaitingAck()).isFalse();
    }

    @Test
    @DisplayName("a non-zero ACK settles it as rejected, not as failed")
    void nonZeroAckIsRejection() throws SQLException {
        UUID id = issue("k-reject", CommandType.TAKEOFF).command().id();

        repository.recordAck(id, 4, Instant.now());

        Command settled = repository.find(TENANT, id).orElseThrow();
        assertThat(settled.state())
                .as("the vehicle understood and refused, which is not the same as no answer")
                .isEqualTo(CommandState.REJECTED);
        assertThat(settled.ack()).hasValue(4);
    }

    /**
     * At-least-once delivery makes duplicate ACKs ordinary, so the second must not overwrite the
     * first — or a rejection followed by a retransmitted acceptance would flip a settled command.
     */
    @Test
    @DisplayName("a duplicate ACK does not overwrite the first answer")
    void duplicateAckIsIgnored() throws SQLException {
        UUID id = issue("k-dup", CommandType.ARM).command().id();
        repository.recordAck(id, 4, Instant.now());

        assertThat(repository.recordAck(id, 0, Instant.now()))
                .as("the command is already settled")
                .isFalse();
        assertThat(repository.find(TENANT, id).orElseThrow().state())
                .isEqualTo(CommandState.REJECTED);
    }

    @Test
    @DisplayName("commands past their window are timed out, and settled ones are left alone")
    void expiryOnlyTouchesUnansweredCommands() throws SQLException {
        UUID unanswered =
                repository
                        .issue(
                                TENANT, DEVICE, "k-expire", CommandType.ARM, Map.of(),
                                "operator@acme", TOPIC, Duration.ofSeconds(-1))
                        .command()
                        .id();
        UUID answered = issue("k-answered", CommandType.ARM).command().id();
        repository.recordAck(answered, 0, Instant.now());

        assertThat(repository.expireStale(Instant.now())).isEqualTo(1);

        assertThat(repository.find(TENANT, unanswered).orElseThrow().state())
                .isEqualTo(CommandState.TIMED_OUT);
        assertThat(repository.find(TENANT, answered).orElseThrow().state())
                .as("an answered command cannot later time out")
                .isEqualTo(CommandState.ACKED);
    }

    @Test
    @DisplayName("a command cannot be read from another tenant")
    void findIsTenantScoped() throws SQLException {
        UUID id = issue("k-tenant", CommandType.ARM).command().id();

        assertThat(repository.find(TENANT, id)).isPresent();
        assertThat(repository.find("other", id))
                .as("the tenant is part of the lookup, not a check applied to the result")
                .isEmpty();
    }

    @Test
    @DisplayName("parameters survive the round trip through the database")
    void parametersRoundTrip() throws SQLException {
        CommandRepository.Issued issued =
                repository.issue(
                        TENANT,
                        DEVICE,
                        "k-params",
                        CommandType.TAKEOFF,
                        Map.of("param7", 25.5),
                        "operator@acme",
                        TOPIC,
                        TIMEOUT);

        Command stored = repository.find(TENANT, issued.command().id()).orElseThrow();
        assertThat(stored.params()).containsEntry("param7", 25.5);
        assertThat(stored.type()).isEqualTo(CommandType.TAKEOFF);
    }

    @Test
    @DisplayName("ARM and DISARM are distinguished in the outbox payload, not by MAV_CMD id")
    void armAndDisarmShareACommandId() throws SQLException {
        issue("k-arm", CommandType.ARM);
        issue("k-disarm", CommandType.DISARM);

        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                """
                                SELECT payload->>'mavCommandId' AS cmd,
                                       payload->'params'->>'param1' AS param1
                                  FROM command_outbox ORDER BY id
                                """)) {
            rows.next();
            assertThat(rows.getString("cmd")).isEqualTo("400");
            assertThat(Double.parseDouble(rows.getString("param1"))).isEqualTo(1.0);

            rows.next();
            assertThat(rows.getString("cmd"))
                    .as("the same MAV_CMD id as ARM — only param1 differs")
                    .isEqualTo("400");
            assertThat(Double.parseDouble(rows.getString("param1"))).isEqualTo(0.0);
        }
    }
}
