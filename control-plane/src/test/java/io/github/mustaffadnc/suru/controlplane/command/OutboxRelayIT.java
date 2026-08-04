package io.github.mustaffadnc.suru.controlplane.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mustaffadnc.suru.controlplane.audit.AuditLog;
import io.github.mustaffadnc.suru.storage.TelemetrySchema;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** The relay: what it publishes, what it marks, and what it leaves alone when publication fails. */
class OutboxRelayIT {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("timescale/timescaledb:2.29.0-pg17")
                    .asCompatibleSubstituteFor("postgres");

    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";
    private static final String TOPIC = "commands.outbound";

    private static PostgreSQLContainer container;
    private static HikariDataSource dataSource;
    private static CommandRepository repository;

    /** Records what was published, and can be told to fail. */
    private static final class RecordingPublisher implements OutboxRelay.Publisher {
        private final List<String> keys = new CopyOnWriteArrayList<>();
        private final List<String> payloads = new CopyOnWriteArrayList<>();
        private volatile boolean broken;

        @Override
        public void publish(String topic, String key, String payload) throws Exception {
            if (broken) {
                throw new IllegalStateException("broker unavailable");
            }
            keys.add(key);
            payloads.add(payload);
        }
    }

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
        repository = new CommandRepository(dataSource, new AuditLog(dataSource));
        execute("INSERT INTO tenant (tenant_id, display_name) VALUES ('acme', 'Acme')");
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
    void clear() {
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

    private static String queryOne(String sql) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            return rows.next() ? rows.getString(1) : null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private UUID issue(String key) throws SQLException {
        return issue(key, DEVICE);
    }

    /**
     * Issues an ARM to a named device.
     *
     * <p>Tests needing several queued commands give each its own device: only one unanswered
     * command per {@code (device, MAV_CMD id)} is allowed, because a COMMAND_ACK carries no
     * correlation id and two ARMs to one vehicle would produce an answer that matched both. These
     * tests are about the relay's batching, not about that guard.
     */
    private UUID issue(String key, String device) throws SQLException {
        return repository
                .issue(
                        TENANT, device, key, CommandType.ARM, Map.of(), "operator@acme", TOPIC,
                        Duration.ofSeconds(30))
                .command()
                .id();
    }

    private static OutboxRelay relay(OutboxRelay.Publisher publisher) {
        return new OutboxRelay(dataSource, publisher, 10, Duration.ofMillis(10));
    }

    @Test
    @DisplayName("an unpublished row is published, marked, and its command becomes SENT")
    void publishesAndMarks() throws SQLException {
        UUID id = issue("k1");
        RecordingPublisher publisher = new RecordingPublisher();

        try (OutboxRelay relay = relay(publisher)) {
            assertThat(relay.publishBatch()).isEqualTo(1);
        }

        assertThat(publisher.payloads).hasSize(1);
        assertThat(publisher.keys)
                .as("keyed by device so one vehicle's commands stay in order on one partition")
                .containsExactly(TENANT + '/' + DEVICE);
        assertThat(queryOne("SELECT count(*) FROM command_outbox WHERE published_at IS NULL"))
                .isEqualTo("0");
        assertThat(repository.find(TENANT, id).orElseThrow().state())
                .isEqualTo(CommandState.SENT);
    }

    @Test
    @DisplayName("a published row is never published again")
    void doesNotRepublish() throws SQLException {
        issue("k2");
        RecordingPublisher publisher = new RecordingPublisher();

        try (OutboxRelay relay = relay(publisher)) {
            assertThat(relay.publishBatch()).isEqualTo(1);
            assertThat(relay.publishBatch()).isZero();
            assertThat(relay.publishBatch()).isZero();
        }

        assertThat(publisher.payloads).hasSize(1);
    }

    /**
     * The reason publication comes before the mark.
     *
     * <p>Marking first would lose this command permanently: the row would read as published, no
     * relay would look at it again, and the operator would see a dispatched command that never
     * left. Leaving it unmarked costs a possible duplicate, which the idempotency key exists for.
     */
    @Test
    @DisplayName("a failed publication leaves the row unpublished so it is retried")
    void failedPublicationIsRetried() throws SQLException {
        UUID id = issue("k3");
        RecordingPublisher publisher = new RecordingPublisher();
        publisher.broken = true;

        try (OutboxRelay relay = relay(publisher)) {
            assertThat(relay.publishBatch()).isZero();

            assertThat(queryOne("SELECT count(*) FROM command_outbox WHERE published_at IS NULL"))
                    .as("still claimable")
                    .isEqualTo("1");
            assertThat(repository.find(TENANT, id).orElseThrow().state())
                    .as("a command that was not sent must not read as SENT")
                    .isEqualTo(CommandState.PENDING);

            publisher.broken = false;
            assertThat(relay.publishBatch()).isEqualTo(1);
        }

        assertThat(repository.find(TENANT, id).orElseThrow().state())
                .isEqualTo(CommandState.SENT);
    }

    @Test
    @DisplayName("a batch stops at the first failure rather than skipping past it")
    void stopsAtTheFirstFailure() throws SQLException {
        issue("a", "link/sysA");
        issue("b", "link/sysB");
        issue("c", "link/sysC");

        List<String> publishedKeys = new ArrayList<>();
        OutboxRelay.Publisher failsOnSecond =
                (topic, key, payload) -> {
                    if (publishedKeys.size() == 1) {
                        throw new IllegalStateException("broker unavailable");
                    }
                    publishedKeys.add(payload);
                };

        try (OutboxRelay relay = relay(failsOnSecond)) {
            assertThat(relay.publishBatch()).isEqualTo(1);
        }

        assertThat(queryOne("SELECT count(*) FROM command_outbox WHERE published_at IS NULL"))
                .as("skipping ahead would deliver a later command before an earlier one")
                .isEqualTo("2");
    }

    /**
     * Two relays must not publish the same row.
     *
     * <p>The first relay's transaction is held open while the second runs, which is exactly the
     * window {@code SKIP LOCKED} exists to cover: without it the second would either block behind
     * the first or read and publish the same rows.
     */
    @Test
    @DisplayName("a second relay skips rows the first is holding")
    void skipLockedKeepsRelaysDisjoint() throws Exception {
        issue("x", "link/sysX");
        issue("y", "link/sysY");

        try (Connection held = dataSource.getConnection()) {
            held.setAutoCommit(false);
            try (Statement statement = held.createStatement();
                    ResultSet rows =
                            statement.executeQuery(
                                    """
                                    SELECT id FROM command_outbox
                                     WHERE published_at IS NULL
                                     ORDER BY id LIMIT 1
                                       FOR UPDATE SKIP LOCKED
                                    """)) {
                assertThat(rows.next()).as("the first relay claims one row").isTrue();
            }

            RecordingPublisher publisher = new RecordingPublisher();
            try (OutboxRelay second = relay(publisher)) {
                assertThat(second.publishBatch())
                        .as("the held row is skipped, the other is taken")
                        .isEqualTo(1);
            }
            held.rollback();
        }

        assertThat(queryOne("SELECT count(*) FROM command_outbox WHERE published_at IS NULL"))
                .as("the released row is still waiting for whoever picks it up next")
                .isEqualTo("1");
    }

    @Test
    @DisplayName("an empty outbox is a no-op")
    void emptyOutbox() throws SQLException {
        RecordingPublisher publisher = new RecordingPublisher();

        try (OutboxRelay relay = relay(publisher)) {
            assertThat(relay.publishBatch()).isZero();
        }
        assertThat(publisher.payloads).isEmpty();
    }
}
