package io.github.mustaffadnc.suru.storage;

import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkPayload;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
 * Reads telemetry off Kafka, decodes it into measurements and bulk-writes them.
 *
 * <p><b>Offsets are committed after the database transaction, never before.</b> That ordering is
 * the whole design of this class. Committing first would make a crash between the two lose a batch
 * outright — silently, because Kafka would consider it delivered and nothing would ever ask for it
 * again. Committing second means a crash replays the batch, and duplicate rows are recoverable in a
 * way that missing ones are not. The write path is built to tolerate exactly that; see
 * {@link TelemetryCopyWriter}.
 *
 * <p>Auto-commit is disabled for the same reason: it commits on a timer, with no relationship to
 * whether the data reached the database.
 *
 * <p><b>Not every record produces rows.</b> The gateway publishes every frame it accepts, including
 * message types the platform does not store — heartbeats, parameter traffic, simulator ground
 * truth. Those pass through leaving nothing behind, which is intended: the frame is still in Kafka
 * and in cold storage if anyone ever needs it.
 */
public final class TelemetryIngestService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestService.class);

    /**
     * Where a decoded batch goes.
     *
     * <p>Narrower than {@link TelemetryCopyWriter} so a test can inject a writer that fails, which
     * is the only way to exercise the property this class exists to guarantee: that a failed write
     * leaves offsets uncommitted and the batch comes back.
     */
    @FunctionalInterface
    public interface BatchWriter {
        /**
         * Writes a batch.
         *
         * @param rows the measurements
         * @return how many rows were written
         * @throws SQLException if the write fails
         */
        long write(List<TelemetryRow> rows) throws SQLException;
    }

    private final Consumer<String, byte[]> consumer;
    private final BatchWriter writer;
    private final Duration pollTimeout;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean loopActive = new AtomicBoolean();
    private final LongAdder recordsConsumed = new LongAdder();
    private final LongAdder rowsWritten = new LongAdder();
    private final LongAdder batchesWritten = new LongAdder();
    private final LongAdder recordsWithoutMetrics = new LongAdder();

    /**
     * Creates a service around a supplied consumer, for tests.
     *
     * @param consumer subscribed consumer; this service takes ownership and closes it
     * @param writer the bulk writer
     * @param pollTimeout how long each poll waits
     */
    public TelemetryIngestService(
            Consumer<String, byte[]> consumer, BatchWriter writer, Duration pollTimeout) {
        this.consumer = consumer;
        this.writer = writer;
        this.pollTimeout = pollTimeout;
    }

    /**
     * Consumer settings for this service.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId consumer group
     * @return the properties
     */
    public static Properties defaultProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        // Offsets are committed by this class, after the write. A timer-based commit has no
        // relationship to whether the data reached the database.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Large polls: the point of this consumer is to hand the database big COPY batches, and
        // a poll that returns a handful of records produces a batch too small to amortise the
        // round trip.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10_000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 64 * 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
        return props;
    }

    /**
     * Creates a service with a new consumer subscribed to a topic.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId consumer group
     * @param topic topic to consume
     * @param writer the bulk writer
     * @return the service
     */
    public static TelemetryIngestService create(
            String bootstrapServers, String groupId, String topic, BatchWriter writer) {
        KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(defaultProperties(bootstrapServers, groupId));
        consumer.subscribe(List.of(topic));
        return new TelemetryIngestService(consumer, writer, Duration.ofMillis(500));
    }

    /**
     * Polls once, writes whatever came back, and commits.
     *
     * @return how many measurement rows were written
     * @throws SQLException if the write fails, in which case offsets are deliberately not
     *     committed and the batch will be redelivered
     */
    public long pollOnce() throws SQLException {
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        if (records.isEmpty()) {
            return 0;
        }

        List<TelemetryRow> rows = new ArrayList<>(records.count() * 4);
        for (ConsumerRecord<String, byte[]> record : records) {
            recordsConsumed.increment();
            int before = rows.size();
            toRows(record, rows);
            if (rows.size() == before) {
                recordsWithoutMetrics.increment();
            }
        }

        long written = writer.write(rows);

        // Only now. A failure above leaves the offsets where they were and the batch comes back.
        consumer.commitSync();

        rowsWritten.add(written);
        batchesWritten.increment();
        return written;
    }

    /** Polls until {@link #close()} is called. */
    public void runUntilClosed() {
        loopActive.set(true);
        try {
            while (running.get()) {
                try {
                    pollOnce();
                } catch (SQLException e) {
                    // Do not commit, do not drop the batch: back off and let it be redelivered.
                    log.error("write failed, batch will be redelivered", e);
                    sleepBriefly();
                }
            }
        } catch (WakeupException expected) {
            log.info("ingest service woken for shutdown");
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

    /** Converts one Kafka record into measurement rows. */
    private static void toRows(ConsumerRecord<String, byte[]> record, List<TelemetryRow> out) {
        String tenantId = header(record, KafkaTelemetryPublisher.HEADER_TENANT);
        String deviceId = header(record, KafkaTelemetryPublisher.HEADER_DEVICE);
        String messageId = header(record, KafkaTelemetryPublisher.HEADER_MESSAGE_ID);
        if (tenantId == null || deviceId == null || messageId == null) {
            // A record without routing headers cannot be attributed to anything. Dropping it is
            // the only option, but it is worth a log line rather than a silent skip.
            log.warn(
                    "record at {}-{} offset {} lacks routing headers",
                    record.topic(),
                    record.partition(),
                    record.offset());
            return;
        }

        Instant time = receivedAt(record);
        MavlinkPayload payload = MavlinkPayload.of(record.value());
        MavlinkMetrics.extract(
                Integer.parseInt(messageId),
                payload,
                (metric, value) -> out.add(new TelemetryRow(time, tenantId, deviceId, metric, value)));
    }

    /**
     * The gateway's receive time, falling back to the broker's timestamp.
     *
     * <p>The gateway's clock is closer to when the vehicle actually transmitted; the broker's is
     * only when the record arrived. The fallback exists because a record can reach this consumer
     * from a producer that predates the header.
     */
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
     * Counters for what this service has processed.
     *
     * @return a snapshot
     */
    public IngestStats stats() {
        return new IngestStats(
                recordsConsumed.sum(),
                rowsWritten.sum(),
                batchesWritten.sum(),
                recordsWithoutMetrics.sum());
    }

    /**
     * Stops the service and releases the consumer.
     *
     * <p>Which of the two paths applies matters. A {@link KafkaConsumer} is not thread-safe, so
     * when {@link #runUntilClosed()} is polling on another thread the only legal action is
     * {@code wakeup()} and the loop closes it on the way out. When nobody is polling —
     * {@link #pollOnce()} used directly, as tests do — nothing would ever close it, and an
     * unclosed consumer keeps its partition assignment: the next consumer in the same group sits
     * waiting for a rebalance that only happens once the session times out, and reads nothing in
     * the meantime.
     */
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
     * Ingest counters.
     *
     * @param recordsConsumed Kafka records read
     * @param rowsWritten measurement rows written
     * @param batchesWritten COPY batches issued
     * @param recordsWithoutMetrics records carrying no stored measurement, which is normal
     */
    public record IngestStats(
            long recordsConsumed, long rowsWritten, long batchesWritten, long recordsWithoutMetrics) {

        /**
         * Average rows produced per Kafka record.
         *
         * @return the ratio, or {@code 0.0} when nothing has been consumed
         */
        public double rowsPerRecord() {
            return recordsConsumed == 0 ? 0.0 : (double) rowsWritten / recordsConsumed;
        }

        @Override
        public String toString() {
            return "records=%d rows=%d batches=%d noMetrics=%d (%.2f rows/record)"
                    .formatted(
                            recordsConsumed,
                            rowsWritten,
                            batchesWritten,
                            recordsWithoutMetrics,
                            rowsPerRecord());
        }
    }
}
