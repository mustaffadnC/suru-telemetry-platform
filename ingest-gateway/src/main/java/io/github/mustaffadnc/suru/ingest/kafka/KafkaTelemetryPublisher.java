package io.github.mustaffadnc.suru.ingest.kafka;

import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import io.github.mustaffadnc.suru.ingest.TelemetryPublisher;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Publishes telemetry to Kafka.
 *
 * <p><b>Record shape.</b> The key is {@code tenant/device}; the value is the payload exactly as it
 * came off the wire; everything else travels in headers. No schema is introduced at this layer, and
 * that is deliberate — the payload's meaning is defined by the message id, which a consumer must
 * look up anyway, so wrapping it in JSON would add encoding cost, inflate every record, and create
 * a second definition of the same thing to keep in step with the protocol module. Headers keep the
 * routing metadata inspectable from {@code kafka-console-consumer} without touching the payload.
 *
 * <p><b>Keying by device is not cosmetic.</b> One device's messages must land on one partition, in
 * order: every downstream sequence-gap calculation, state machine and windowed aggregate assumes it.
 * Round-robin partitioning would silently reorder a vehicle's telemetry across partitions and make
 * those computations wrong in ways that look like packet loss.
 *
 * <p>The producer is configured idempotent with {@code acks=all}, so a retry after a network blip
 * cannot duplicate a record and a record is not acknowledged until every in-sync replica holds it.
 * The returned stage completes on that acknowledgement rather than on hand-off, which is what makes
 * admission control's pressure signal mean anything.
 */
public final class KafkaTelemetryPublisher implements TelemetryPublisher {

    /** Header carrying the owning tenant. */
    public static final String HEADER_TENANT = "tenant";

    /** Header carrying the device id. */
    public static final String HEADER_DEVICE = "device";

    /** Header carrying the wire protocol the message was framed with. */
    public static final String HEADER_SOURCE = "source";

    /** Header carrying the protocol message id. */
    public static final String HEADER_MESSAGE_ID = "msgId";

    /** Header carrying the per-endpoint sequence number. */
    public static final String HEADER_SEQUENCE = "seq";

    /** Header carrying the MAVLink system id. */
    public static final String HEADER_SYSTEM_ID = "sysId";

    /** Header carrying the MAVLink component id. */
    public static final String HEADER_COMPONENT_ID = "compId";

    /** Header carrying the shedding band the message was classified into. */
    public static final String HEADER_PRIORITY = "priority";

    /** Header carrying the gateway receive timestamp, in nanoseconds. */
    public static final String HEADER_RECEIVED_AT = "receivedAtNanos";

    private final Producer<String, byte[]> producer;
    private final String topic;

    /**
     * Creates a publisher with production-shaped producer settings.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic destination topic
     */
    public KafkaTelemetryPublisher(String bootstrapServers, String topic) {
        this(new KafkaProducer<>(defaultProperties(bootstrapServers)), topic);
    }

    /**
     * Creates a publisher around a supplied producer, for tests.
     *
     * @param producer the producer to use; this publisher takes ownership and closes it
     * @param topic destination topic
     */
    public KafkaTelemetryPublisher(Producer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    /**
     * The producer configuration used by the convenience constructor.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @return the properties
     */
    public static Properties defaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // A retried send must not become a second record. Without this, the ordinary case of a
        // transient network error turns into duplicate telemetry that every downstream
        // aggregate then double-counts.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Telemetry frames are tens of bytes; sending them individually would spend far more on
        // per-request overhead than on data. A few milliseconds of linger buys large batches and
        // is invisible next to the flight timescales this platform reasons about.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Bounded so a stalled broker surfaces as backpressure rather than as unbounded growth
        // inside the producer, where admission control cannot see it.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        return props;
    }

    @Override
    public CompletionStage<Void> publish(TelemetryEnvelope envelope) {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, envelope.partitionKey(), envelope.payload());

        var headers = record.headers();
        headers.add(HEADER_TENANT, bytes(envelope.tenantId()));
        headers.add(HEADER_DEVICE, bytes(envelope.deviceId()));
        headers.add(HEADER_SOURCE, bytes(envelope.source().name()));
        headers.add(HEADER_MESSAGE_ID, bytes(Integer.toString(envelope.messageId())));
        headers.add(HEADER_SEQUENCE, bytes(Integer.toString(envelope.sequence())));
        headers.add(HEADER_SYSTEM_ID, bytes(Integer.toString(envelope.systemId())));
        headers.add(HEADER_COMPONENT_ID, bytes(Integer.toString(envelope.componentId())));
        headers.add(HEADER_PRIORITY, bytes(envelope.priority().name()));
        headers.add(HEADER_RECEIVED_AT, bytes(Long.toString(envelope.receivedAtEpochNanos())));

        CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        producer.send(
                record,
                (metadata, exception) -> {
                    if (exception != null) {
                        acknowledged.completeExceptionally(exception);
                    } else {
                        acknowledged.complete(null);
                    }
                });
        return acknowledged;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Blocks until every record handed to this publisher has been acknowledged.
     *
     * <p>Records linger in the producer's batch buffer by design, so "the gateway finished
     * reading" and "the broker has the data" are different moments. Anything that needs the second
     * — a test that counts records, a graceful shutdown — must ask for it rather than wait and
     * hope.
     */
    public void flush() {
        producer.flush();
    }

    /**
     * Producer metrics, for wiring into a meter registry.
     *
     * @return the producer's metrics
     */
    public Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>
            metrics() {
        return producer.metrics();
    }

    @Override
    public void close() {
        producer.close();
    }
}
