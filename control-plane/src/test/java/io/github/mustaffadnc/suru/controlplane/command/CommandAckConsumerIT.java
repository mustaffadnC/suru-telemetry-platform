package io.github.mustaffadnc.suru.controlplane.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mustaffadnc.suru.controlplane.audit.AuditLog;
import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkCommands;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.storage.TelemetrySchema;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Settling commands from the vehicles' answers — the last link in the loop.
 *
 * <p>A {@link MockConsumer} feeds the records and a real database receives the writes: what is
 * under test is which records are recognised as answers and what happens to the command, neither of
 * which Kafka decides.
 */
class CommandAckConsumerIT {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("timescale/timescaledb:2.29.0-pg17")
                    .asCompatibleSubstituteFor("postgres");

    private static final String TOPIC = "telemetry.raw";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";
    private static final int MAV_CMD_ARM_DISARM = 400;
    private static final int MAV_CMD_LAND = 21;

    private static PostgreSQLContainer container;
    private static HikariDataSource dataSource;
    private static CommandRepository repository;
    private static AuditLog auditLog;

    private MockConsumer<String, byte[]> consumer;
    private long nextOffset;

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
        auditLog = new AuditLog(dataSource);
        repository = new CommandRepository(dataSource, auditLog);
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
    void setUp() {
        execute("DELETE FROM command_outbox");
        execute("DELETE FROM command");
        // "latest" matches the real consumer's reset policy, and MockConsumer needs an end offset
        // to seek to for it — without one, poll() throws rather than returning nothing.
        consumer = new MockConsumer<>("latest");
        consumer.assign(List.of(PARTITION));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION, 0L));
        consumer.seek(PARTITION, 0L);
        nextOffset = 0;
    }

    private static void execute(String sql) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("failed: " + sql, e);
        }
    }

    private static long auditRows(String action, String subject) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                "SELECT count(*) FROM audit_log WHERE action = '%s' AND subject = '%s'"
                                        .formatted(action, subject))) {
            rows.next();
            return rows.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private UUID issue(String key, CommandType type, String device) throws SQLException {
        return repository
                .issue(
                        TENANT, device, key, type, Map.of(), "operator@acme", "commands.outbound",
                        Duration.ofSeconds(30))
                .command()
                .id();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** Feeds one record with the headers the gateway writes. */
    private void publish(int messageId, byte[] payload, String tenant, String device) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_TENANT, bytes(tenant)));
        headers.add(new RecordHeader(KafkaTelemetryPublisher.HEADER_DEVICE, bytes(device)));
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_MESSAGE_ID,
                        bytes(Integer.toString(messageId))));
        long nanos = Instant.now().getEpochSecond() * 1_000_000_000L;
        headers.add(
                new RecordHeader(
                        KafkaTelemetryPublisher.HEADER_RECEIVED_AT, bytes(Long.toString(nanos))));

        consumer.addRecord(
                new ConsumerRecord<>(
                        TOPIC, 0, nextOffset++, 0L,
                        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                        0, payload.length, tenant + '/' + device, payload, headers,
                        java.util.Optional.empty()));
    }

    /** The short form a v2 sender produces: command and result only. */
    private static byte[] ack(int mavCommandId, int result) {
        return new byte[] {
            (byte) (mavCommandId & 0xFF), (byte) ((mavCommandId >>> 8) & 0xFF), (byte) result
        };
    }

    private CommandAckConsumer consumerUnderTest() {
        return new CommandAckConsumer(consumer, repository, auditLog, Duration.ofMillis(10));
    }

    @Test
    @DisplayName("an accepted ACK settles the command and is audited")
    void acceptedAckSettlesTheCommand() throws SQLException {
        // Counted as a delta: audit_log refuses DELETE, so rows from other tests in this class are
        // still there. That the cleanup cannot remove them is the append-only design working.
        long auditedBefore = auditRows("command.ack", DEVICE);

        UUID id = issue("ack-1", CommandType.ARM, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), TENANT, DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            assertThat(acks.pollOnce()).isEqualTo(1);
            assertThat(acks.stats().commandsSettled()).isEqualTo(1);
        }

        assertThat(repository.find(TENANT, id).orElseThrow().state())
                .isEqualTo(CommandState.ACKED);
        assertThat(auditRows("command.ack", DEVICE))
                .as("the vehicle's answer belongs in the record as much as the request did")
                .isEqualTo(auditedBefore + 1);
    }

    @Test
    @DisplayName("a non-zero result settles the command as rejected")
    void rejectionIsRecordedAsSuch() throws SQLException {
        UUID id = issue("ack-2", CommandType.LAND, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_LAND, 4), TENANT, DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            acks.pollOnce();
        }

        Command settled = repository.find(TENANT, id).orElseThrow();
        assertThat(settled.state()).isEqualTo(CommandState.REJECTED);
        assertThat(settled.ack()).hasValue(4);
    }

    /**
     * The reason this consumer can read the whole telemetry topic without it mattering: nearly
     * every record is discarded on a header comparison, without the payload being touched.
     */
    @Test
    @DisplayName("ordinary telemetry is discarded without being decoded")
    void nonAckTelemetryIsIgnored() throws SQLException {
        issue("ack-3", CommandType.ARM, DEVICE);
        for (int i = 0; i < 50; i++) {
            publish(MavlinkMetrics.MSG_GLOBAL_POSITION_INT, new byte[28], TENANT, DEVICE);
        }
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), TENANT, DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            acks.pollOnce();

            CommandAckConsumer.AckStats stats = acks.stats();
            assertThat(stats.recordsSeen()).isEqualTo(51);
            assertThat(stats.acksSeen()).isEqualTo(1);
            assertThat(stats.commandsSettled()).isEqualTo(1);
            assertThat(stats.ackRatio()).isLessThan(0.02);
        }
    }

    @Test
    @DisplayName("an ACK matching no open command is counted, not an error")
    void unmatchedAckIsOrdinary() throws SQLException {
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), TENANT, DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            assertThat(acks.pollOnce()).isZero();
            assertThat(acks.stats().unmatched())
                    .as("another ground station's command, or an answer after the timeout")
                    .isEqualTo(1);
        }
    }

    @Test
    @DisplayName("a duplicate ACK does not disturb a settled command")
    void duplicateAckIsHarmless() throws SQLException {
        UUID id = issue("ack-4", CommandType.ARM, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), TENANT, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 4), TENANT, DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            assertThat(acks.pollOnce())
                    .as("the second answer finds nothing left to settle")
                    .isEqualTo(1);
        }

        assertThat(repository.find(TENANT, id).orElseThrow().state())
                .as("a retransmitted rejection must not overturn an acceptance")
                .isEqualTo(CommandState.ACKED);
    }

    @Test
    @DisplayName("an ACK from one device does not settle another's command")
    void ackIsScopedToItsDevice() throws SQLException {
        UUID mine = issue("ack-5", CommandType.ARM, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), TENANT, "link/sys9");

        try (CommandAckConsumer acks = consumerUnderTest()) {
            assertThat(acks.pollOnce()).isZero();
        }

        assertThat(repository.find(TENANT, mine).orElseThrow().state())
                .isEqualTo(CommandState.PENDING);
    }

    @Test
    @DisplayName("an ACK under another tenant settles nothing")
    void ackIsTenantScoped() throws SQLException {
        execute(
                """
                INSERT INTO tenant (tenant_id, display_name) VALUES ('rival', 'Rival')
                ON CONFLICT DO NOTHING
                """);
        UUID mine = issue("ack-6", CommandType.ARM, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), "rival", DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            assertThat(acks.pollOnce()).isZero();
        }

        assertThat(repository.find(TENANT, mine).orElseThrow().state())
                .isEqualTo(CommandState.PENDING);
    }

    @Test
    @DisplayName("offsets advance only after the batch is settled")
    void offsetsCommitAfterTheWrites() throws SQLException {
        issue("ack-7", CommandType.ARM, DEVICE);
        publish(MavlinkCommands.MSG_COMMAND_ACK, ack(MAV_CMD_ARM_DISARM, 0), TENANT, DEVICE);

        try (CommandAckConsumer acks = consumerUnderTest()) {
            acks.pollOnce();
            assertThat(consumer.committed(Set.of(PARTITION)).get(PARTITION).offset())
                    .as("a crash before this leaves the ACK to be replayed, which is harmless")
                    .isEqualTo(1);
        }
    }
}
