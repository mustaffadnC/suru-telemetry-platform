package io.github.mustaffadnc.suru.controlplane.command;

import io.github.mustaffadnc.suru.controlplane.audit.AuditEntry;
import io.github.mustaffadnc.suru.controlplane.audit.AuditLog;
import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkCommands;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkPayload;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Settles commands from the vehicles' COMMAND_ACK messages.
 *
 * <p>This closes the loop: an operator's request becomes a command row, an outbox row, a Kafka
 * record, a COMMAND_LONG frame — and then the vehicle answers, and until that answer is written
 * back the command is only a hope.
 *
 * <h2>It reads the whole telemetry topic</h2>
 *
 * <p>ACKs are not published separately; they arrive mixed into ordinary telemetry, so this consumer
 * sees every frame the fleet sends and discards nearly all of them. That looks wasteful and is
 * acceptable here for a reason worth stating rather than assuming: the discard is a header
 * comparison on a byte array that is never deserialised, and the fleet rate this platform targets
 * is around a thousand messages a second — three orders of magnitude under the gateway's measured
 * capacity and well under the storage consumer's.
 *
 * <p>The alternative, having the gateway republish ACKs to a topic of their own, is cheap there
 * because it already decodes every frame. It was not done because it puts knowledge of commands
 * into the ingest path, and the saving is currently theoretical. <b>The trigger to revisit is a
 * real number:</b> if this consumer's lag becomes visible, or the fleet rate approaches the tens of
 * thousands per second where the header scan starts to cost real CPU, the dedicated topic is the
 * answer.
 */
public final class CommandAckConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CommandAckConsumer.class);

    private final Consumer<String, byte[]> consumer;
    private final CommandRepository repository;
    private final AuditLog auditLog;
    private final Duration pollTimeout;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean loopActive = new AtomicBoolean();

    private final LongAdder recordsSeen = new LongAdder();
    private final LongAdder acksSeen = new LongAdder();
    private final LongAdder commandsSettled = new LongAdder();
    private final LongAdder unmatched = new LongAdder();

    /**
     * Creates a consumer around a supplied Kafka consumer, for tests.
     *
     * @param consumer subscribed consumer; this class takes ownership and closes it
     * @param repository where commands are settled
     * @param auditLog where the outcome is recorded
     * @param pollTimeout how long each poll waits
     */
    public CommandAckConsumer(
            Consumer<String, byte[]> consumer,
            CommandRepository repository,
            AuditLog auditLog,
            Duration pollTimeout) {
        this.consumer = consumer;
        this.repository = repository;
        this.auditLog = auditLog;
        this.pollTimeout = pollTimeout;
    }

    /**
     * Creates a consumer subscribed to the telemetry topic.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId consumer group
     * @param telemetryTopic the topic the gateway publishes to
     * @param repository where commands are settled
     * @param auditLog where the outcome is recorded
     * @return the consumer
     */
    public static CommandAckConsumer create(
            String bootstrapServers,
            String groupId,
            String telemetryTopic,
            CommandRepository repository,
            AuditLog auditLog) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Latest, not earliest. An ACK is only meaningful against a command still awaiting one, so
        // replaying the topic from the beginning would spend a long time matching nothing --
        // and any command old enough to be in that history has long since timed out.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(telemetryTopic));
        return new CommandAckConsumer(
                consumer, repository, auditLog, Duration.ofMillis(500));
    }

    /**
     * Polls once and settles whatever the vehicles answered.
     *
     * @return how many commands this call settled
     * @throws SQLException if a database write fails, in which case offsets are not committed
     */
    public int pollOnce() throws SQLException {
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        if (records.isEmpty()) {
            return 0;
        }
        int settled = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
            recordsSeen.increment();
            if (settle(record)) {
                settled++;
            }
        }

        // After the writes, as everywhere else: a crash between them replays the batch, and a
        // duplicate ACK is harmless because recordAckFromVehicle only touches an unanswered
        // command.
        consumer.commitSync();
        return settled;
    }

    private boolean settle(ConsumerRecord<String, byte[]> record) throws SQLException {
        String messageId = header(record, KafkaTelemetryPublisher.HEADER_MESSAGE_ID);
        if (messageId == null
                || Integer.parseInt(messageId) != MavlinkCommands.MSG_COMMAND_ACK) {
            return false;
        }
        acksSeen.increment();

        String tenantId = header(record, KafkaTelemetryPublisher.HEADER_TENANT);
        String deviceId = header(record, KafkaTelemetryPublisher.HEADER_DEVICE);
        if (tenantId == null || deviceId == null) {
            log.warn("COMMAND_ACK at offset {} lacks routing headers", record.offset());
            return false;
        }

        MavlinkPayload payload = MavlinkPayload.of(record.value());
        int mavCommandId = MavlinkCommands.ackCommand(payload);
        int result = MavlinkCommands.ackResult(payload);
        Instant at = receivedAt(record);

        Optional<UUID> commandId =
                repository.recordAckFromVehicle(tenantId, deviceId, mavCommandId, result, at);
        if (commandId.isEmpty()) {
            // Ordinary rather than alarming: another ground station's command, a duplicate ACK for
            // one already settled, or an answer that arrived after the command timed out.
            unmatched.increment();
            log.debug(
                    "COMMAND_ACK for MAV_CMD {} from {}/{} matched no open command",
                    mavCommandId,
                    tenantId,
                    deviceId);
            return false;
        }

        commandsSettled.increment();
        auditLog.record(
                AuditEntry.allowed(
                        tenantId,
                        deviceId,
                        "command.ack",
                        deviceId,
                        Map.of(
                                "commandId", commandId.get().toString(),
                                "mavCommandId", Integer.toString(mavCommandId),
                                "result", Integer.toString(result))));
        return true;
    }

    /** Polls until {@link #close()} is called. */
    public void runUntilClosed() {
        loopActive.set(true);
        try {
            while (running.get()) {
                try {
                    pollOnce();
                } catch (SQLException e) {
                    log.error("failed to settle a batch of ACKs, will retry", e);
                    sleepBriefly();
                }
            }
        } catch (WakeupException expected) {
            log.info("ACK consumer woken for shutdown");
        } finally {
            loopActive.set(false);
            consumer.close();
        }
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(Duration.ofSeconds(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Instant receivedAt(ConsumerRecord<String, byte[]> record) {
        String nanos = header(record, KafkaTelemetryPublisher.HEADER_RECEIVED_AT);
        if (nanos != null) {
            long value = Long.parseLong(nanos);
            return Instant.ofEpochSecond(
                    Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L));
        }
        return Instant.ofEpochMilli(record.timestamp());
    }

    private static String header(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * What this consumer has seen.
     *
     * @return a snapshot
     */
    public AckStats stats() {
        return new AckStats(
                recordsSeen.sum(), acksSeen.sum(), commandsSettled.sum(), unmatched.sum());
    }

    @Override
    public void close() {
        running.set(false);
        if (loopActive.get()) {
            consumer.wakeup();
        } else {
            consumer.close();
        }
    }

    /**
     * ACK counters.
     *
     * @param recordsSeen every telemetry record examined
     * @param acksSeen records that were COMMAND_ACK
     * @param commandsSettled commands moved out of PENDING or SENT
     * @param unmatched ACKs that matched no open command, which is normal
     */
    public record AckStats(
            long recordsSeen, long acksSeen, long commandsSettled, long unmatched) {

        /**
         * What proportion of examined records were ACKs.
         *
         * @return the ratio, or {@code 0.0} when nothing has been seen
         */
        public double ackRatio() {
            return recordsSeen == 0 ? 0.0 : (double) acksSeen / recordsSeen;
        }
    }
}
