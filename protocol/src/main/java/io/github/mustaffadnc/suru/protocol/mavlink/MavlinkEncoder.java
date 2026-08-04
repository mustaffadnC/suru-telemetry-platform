package io.github.mustaffadnc.suru.protocol.mavlink;

import io.github.mustaffadnc.suru.protocol.Crc16;
import java.util.Objects;

/**
 * Builds MAVLink frames for transmission.
 *
 * <p>The mirror of {@link MavlinkDecoder}, and held to the same standard: a frame this class
 * produces from a decoded frame's parts must be byte-identical to the bytes that frame arrived in.
 * {@code MavlinkEncoderTest} asserts exactly that over the recorded SITL stream — 1058 real frames
 * from a real vehicle, decoded and re-encoded. An encoder checked only against its own decoder is
 * checked against a shared assumption.
 *
 * <p><b>Sequence numbers belong to the sender, not to the message.</b> One counter per
 * {@code (systemId, componentId)} endpoint, wrapping at 256, because that is what the receiver's
 * gap detection counts. A caller that supplies its own sequence per message type would produce a
 * stream that looks, to every receiver on the link, like continuous packet loss.
 */
public final class MavlinkEncoder {

    private static final int V2_HEADER_LENGTH = 10;
    private static final int V1_HEADER_LENGTH = 6;
    private static final int CHECKSUM_LENGTH = 2;

    private final MavlinkDialect dialect;
    private final int systemId;
    private final int componentId;

    private int sequence;

    /**
     * Creates an encoder for one endpoint.
     *
     * @param dialect supplies the {@code CRC_EXTRA} byte per message
     * @param systemId this sender's system id
     * @param componentId this sender's component id
     */
    public MavlinkEncoder(MavlinkDialect dialect, int systemId, int componentId) {
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.systemId = requireByte(systemId, "systemId");
        this.componentId = requireByte(componentId, "componentId");
    }

    /**
     * The sequence number the next frame will carry.
     *
     * @return {@code 0..255}
     */
    public int nextSequence() {
        return sequence;
    }

    /**
     * Encodes a v2 frame, taking the next sequence number.
     *
     * @param messageId the message id
     * @param payload the payload, which is not modified
     * @return the frame bytes, ready to write to a socket
     */
    public byte[] encodeV2(int messageId, byte[] payload) {
        byte[] frame = encodeV2(messageId, payload, sequence);
        sequence = (sequence + 1) & 0xFF;
        return frame;
    }

    /**
     * Encodes a v2 frame with an explicit sequence number.
     *
     * <p>Used by the round-trip test, and by anything replaying a captured stream. Ordinary senders
     * want {@link #encodeV2(int, byte[])} so the endpoint's counter stays coherent.
     *
     * @param messageId the message id
     * @param payload the payload, which is not modified
     * @param sequenceNumber the sequence number to stamp
     * @return the frame bytes
     */
    public byte[] encodeV2(int messageId, byte[] payload, int sequenceNumber) {
        int crcExtra = requireKnown(messageId);
        int length = truncatedLength(payload);

        byte[] frame = new byte[1 + V2_HEADER_LENGTH - 1 + length + CHECKSUM_LENGTH];
        frame[0] = (byte) MavlinkFrame.STX_V2;
        frame[1] = (byte) length;
        frame[2] = 0; // incompat flags: unsigned
        frame[3] = 0; // compat flags
        frame[4] = (byte) requireByte(sequenceNumber, "sequenceNumber");
        frame[5] = (byte) systemId;
        frame[6] = (byte) componentId;
        frame[7] = (byte) (messageId & 0xFF);
        frame[8] = (byte) ((messageId >>> 8) & 0xFF);
        frame[9] = (byte) ((messageId >>> 16) & 0xFF);
        System.arraycopy(payload, 0, frame, 10, length);

        appendChecksum(frame, 1, V2_HEADER_LENGTH - 1 + length, crcExtra);
        return frame;
    }

    /**
     * Encodes a v1 frame with an explicit sequence number.
     *
     * <p>Present so the round-trip test can re-encode whatever it decoded. v1 has no payload
     * truncation and no extension fields, so the payload is written exactly as given.
     *
     * @param messageId the message id, which must fit in a byte
     * @param payload the payload, which is not modified
     * @param sequenceNumber the sequence number to stamp
     * @return the frame bytes
     */
    public byte[] encodeV1(int messageId, byte[] payload, int sequenceNumber) {
        int crcExtra = requireKnown(messageId);
        if (messageId > 0xFF) {
            throw new IllegalArgumentException(
                    "message id " + messageId + " does not fit in a v1 frame");
        }
        if (payload.length > 255) {
            throw new IllegalArgumentException("payload of " + payload.length + " exceeds 255");
        }

        byte[] frame = new byte[1 + V1_HEADER_LENGTH - 1 + payload.length + CHECKSUM_LENGTH];
        frame[0] = (byte) MavlinkFrame.STX_V1;
        frame[1] = (byte) payload.length;
        frame[2] = (byte) requireByte(sequenceNumber, "sequenceNumber");
        frame[3] = (byte) systemId;
        frame[4] = (byte) componentId;
        frame[5] = (byte) messageId;
        System.arraycopy(payload, 0, frame, 6, payload.length);

        appendChecksum(frame, 1, V1_HEADER_LENGTH - 1 + payload.length, crcExtra);
        return frame;
    }

    /**
     * The checksum covers everything after the start byte, then the message's {@code CRC_EXTRA}.
     *
     * <p>MCRF4XX, not X.25 — the two differ only in a final XOR, which is exactly the kind of
     * difference that produces a frame every receiver rejects while looking correct in a debugger.
     */
    private static void appendChecksum(byte[] frame, int offset, int length, int crcExtra) {
        int crc = Crc16.mcrf4xxFast(Crc16.INIT, frame, offset, length);
        crc = Crc16.mcrf4xxFastUpdate(crc, (byte) crcExtra);
        frame[offset + length] = (byte) (crc & 0xFF);
        frame[offset + length + 1] = (byte) ((crc >>> 8) & 0xFF);
    }

    /**
     * v2 payload length after trailing zeros are removed, floored at one byte.
     *
     * <p>The receiver reconstructs the missing bytes as zeros, so truncating shortens the frame
     * without changing its meaning. It has to be byte-exact here because a re-encoded frame that
     * kept the zeros would not match what the vehicle actually sent.
     *
     * <p><b>Truncation stops at one byte, never zero.</b> This was implemented wrongly first —
     * trailing zeros removed all the way down — and the recorded ArduPilot stream is what caught
     * it. Of 1058 real frames, <b>none</b> carries a zero-length payload and eight carry exactly
     * one byte whose value is zero: a VFR_HUD that truncates to nothing is still sent as
     * {@code len=1}. The shortest payload in the whole recording is one byte.
     *
     * <p>Nothing in a round trip through this codebase's own decoder would have shown that, since
     * it reconstructs the zeros either way. Only bytes from a real vehicle disagree.
     */
    private static int truncatedLength(byte[] payload) {
        int length = payload.length;
        while (length > 1 && payload[length - 1] == 0) {
            length--;
        }
        return length;
    }

    private int requireKnown(int messageId) {
        int crcExtra = dialect.crcExtra(messageId);
        if (crcExtra < 0) {
            throw new IllegalArgumentException(
                    "message id " + messageId + " is not in dialect " + dialect.getClass().getSimpleName());
        }
        return crcExtra;
    }

    private static int requireByte(int value, String name) {
        if (value < 0 || value > 0xFF) {
            throw new IllegalArgumentException(name + " must be 0..255 but was " + value);
        }
        return value;
    }
}
