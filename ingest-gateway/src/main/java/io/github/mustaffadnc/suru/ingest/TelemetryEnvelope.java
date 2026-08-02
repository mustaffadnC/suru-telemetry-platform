package io.github.mustaffadnc.suru.ingest;

import java.util.Arrays;
import java.util.Objects;

/**
 * One telemetry message, decoded and attributed to a device, on its way to Kafka.
 *
 * <p>This is the gateway's output type and therefore the boundary where the flyweight ends: the
 * decoder's reusable frame view cannot outlive its callback, so anything published copies out of
 * it. Everything downstream of here works on immutable values.
 *
 * @param tenantId owning tenant, resolved from the device credential
 * @param deviceId the device this arrived from
 * @param source which wire protocol it was framed with
 * @param messageId protocol message or record type id
 * @param sequence per-endpoint sequence number, or {@code -1} where the protocol has none
 * @param systemId MAVLink system id, or {@code -1}
 * @param componentId MAVLink component id, or {@code -1}
 * @param receivedAtEpochNanos gateway receive timestamp
 * @param priority shedding band this message falls in
 * @param payload the raw payload bytes
 */
public record TelemetryEnvelope(
        String tenantId,
        String deviceId,
        SourceProtocol source,
        int messageId,
        int sequence,
        int systemId,
        int componentId,
        long receivedAtEpochNanos,
        MessagePriority priority,
        byte[] payload) {

    /** Which framing the message arrived in. */
    public enum SourceProtocol {
        /** MAVLink v1 or v2. */
        MAVLINK,
        /** ÇARGE capsule log framing. */
        HK
    }

    /** Defensive copy on the way in — the decoder's buffer is reused immediately after. */
    public TelemetryEnvelope {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(deviceId, "deviceId");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(priority, "priority");
        payload = payload.clone();
    }

    /**
     * The Kafka partition key. Partitioning by device keeps one device's messages in order on one
     * partition, which every downstream sequence-gap and state calculation depends on.
     *
     * @return the key
     */
    public String partitionKey() {
        return tenantId + '/' + deviceId;
    }

    /**
     * The payload bytes.
     *
     * @return a copy — callers cannot mutate the envelope
     */
    @Override
    public byte[] payload() {
        return payload.clone();
    }

    /**
     * Payload size without copying it.
     *
     * @return length in bytes
     */
    public int payloadLength() {
        return payload.length;
    }

    // A record's generated equals/hashCode compare arrays by identity; compare by content so
    // envelopes can be used in assertions and deduplication sets.

    @Override
    public boolean equals(Object o) {
        return o instanceof TelemetryEnvelope other
                && messageId == other.messageId
                && sequence == other.sequence
                && systemId == other.systemId
                && componentId == other.componentId
                && receivedAtEpochNanos == other.receivedAtEpochNanos
                && source == other.source
                && priority == other.priority
                && tenantId.equals(other.tenantId)
                && deviceId.equals(other.deviceId)
                && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        tenantId,
                        deviceId,
                        source,
                        messageId,
                        sequence,
                        systemId,
                        componentId,
                        receivedAtEpochNanos,
                        priority);
        return 31 * result + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "TelemetryEnvelope[%s/%s %s msg=%d seq=%d len=%d %s]"
                .formatted(
                        tenantId, deviceId, source, messageId, sequence, payload.length, priority);
    }
}
