package io.github.mustaffadnc.suru.protocol.mavlink;

import java.util.Arrays;
import java.util.Objects;

/**
 * A decoded MAVLink frame.
 *
 * <p><b>Lifetime:</b> the instance handed to a {@link MavlinkDecoder.FrameHandler} is a mutable
 * view owned by the decoder and is only valid for the duration of the callback — the next frame
 * overwrites it. This is deliberate: a telemetry gateway decodes tens of thousands of frames per
 * second and must not allocate one object per frame. Call {@link #copyPayload()} or
 * {@link #toImmutable()} if the data has to outlive the callback.
 */
public final class MavlinkFrame {

    /** Start byte of a MAVLink v1 frame. */
    public static final int STX_V1 = 0xFE;

    /** Start byte of a MAVLink v2 frame. */
    public static final int STX_V2 = 0xFD;

    /** Incompatibility flag marking a signed v2 frame. */
    public static final int INCOMPAT_FLAG_SIGNED = 0x01;

    /** Length of a v2 signature block, in bytes. */
    public static final int SIGNATURE_LENGTH = 13;

    private int version;
    private int sequence;
    private int systemId;
    private int componentId;
    private int messageId;
    private int incompatibleFlags;
    private int compatibleFlags;
    private boolean signed;

    private byte[] buffer = new byte[0];
    private int payloadOffset;
    private int payloadLength;

    MavlinkFrame() {}

    void set(
            int version,
            int sequence,
            int systemId,
            int componentId,
            int messageId,
            int incompatibleFlags,
            int compatibleFlags,
            byte[] buffer,
            int payloadOffset,
            int payloadLength) {
        this.version = version;
        this.sequence = sequence;
        this.systemId = systemId;
        this.componentId = componentId;
        this.messageId = messageId;
        this.incompatibleFlags = incompatibleFlags;
        this.compatibleFlags = compatibleFlags;
        this.signed = (incompatibleFlags & INCOMPAT_FLAG_SIGNED) != 0;
        this.buffer = buffer;
        this.payloadOffset = payloadOffset;
        this.payloadLength = payloadLength;
    }

    /**
     * Protocol version of this frame.
     *
     * @return {@code 1} or {@code 2}
     */
    public int version() {
        return version;
    }

    /**
     * Per-endpoint sequence number, wrapping at 256.
     *
     * @return {@code 0..255}
     */
    public int sequence() {
        return sequence;
    }

    /**
     * Sending system id.
     *
     * @return {@code 0..255}
     */
    public int systemId() {
        return systemId;
    }

    /**
     * Sending component id.
     *
     * @return {@code 0..255}
     */
    public int componentId() {
        return componentId;
    }

    /**
     * Message id: 8-bit in v1, 24-bit in v2.
     *
     * @return the message id
     */
    public int messageId() {
        return messageId;
    }

    /**
     * Incompatibility flags. A receiver that does not understand a set bit must drop the frame;
     * only {@link #INCOMPAT_FLAG_SIGNED} is defined today. Always {@code 0} for v1.
     *
     * @return the flags byte
     */
    public int incompatibleFlags() {
        return incompatibleFlags;
    }

    /**
     * Compatibility flags, which a receiver may ignore. Always {@code 0} for v1.
     *
     * @return the flags byte
     */
    public int compatibleFlags() {
        return compatibleFlags;
    }

    /**
     * Whether the frame carries a signature block.
     *
     * @return {@code true} if signed
     */
    public boolean isSigned() {
        return signed;
    }

    /**
     * Payload length as it appeared on the wire. A v2 sender may drop trailing zero bytes, so this
     * can be shorter than the message's defined length; the missing bytes are implicitly zero.
     *
     * @return payload length in bytes
     */
    public int payloadLength() {
        return payloadLength;
    }

    /**
     * Reads one payload byte without copying.
     *
     * @param index offset into the payload
     * @return the byte value as {@code 0..255}
     * @throws IndexOutOfBoundsException if {@code index} is outside the payload
     */
    public int payloadByte(int index) {
        if (index < 0 || index >= payloadLength) {
            throw new IndexOutOfBoundsException(
                    "payload index " + index + " outside 0.." + (payloadLength - 1));
        }
        return buffer[payloadOffset + index] & 0xFF;
    }

    /**
     * Copies the payload out of the decoder's buffer.
     *
     * @return a fresh array of exactly {@link #payloadLength()} bytes
     */
    public byte[] copyPayload() {
        return Arrays.copyOfRange(buffer, payloadOffset, payloadOffset + payloadLength);
    }

    /**
     * Takes a snapshot that stays valid after the callback returns.
     *
     * @return an immutable copy of this frame
     */
    public Snapshot toImmutable() {
        return new Snapshot(
                version,
                sequence,
                systemId,
                componentId,
                messageId,
                incompatibleFlags,
                compatibleFlags,
                copyPayload());
    }

    @Override
    public String toString() {
        return "MavlinkFrame[v"
                + version
                + " sys="
                + systemId
                + " comp="
                + componentId
                + " msg="
                + messageId
                + " seq="
                + sequence
                + " len="
                + payloadLength
                + (signed ? " signed" : "")
                + "]";
    }

    /**
     * An immutable copy of a frame, safe to keep and to hand to another thread.
     *
     * @param version protocol version, {@code 1} or {@code 2}
     * @param sequence per-endpoint sequence number
     * @param systemId sending system id
     * @param componentId sending component id
     * @param messageId message id
     * @param incompatibleFlags v2 incompatibility flags
     * @param compatibleFlags v2 compatibility flags
     * @param payload the payload bytes
     */
    public record Snapshot(
            int version,
            int sequence,
            int systemId,
            int componentId,
            int messageId,
            int incompatibleFlags,
            int compatibleFlags,
            byte[] payload) {

        /** Defensive copy on the way in. */
        public Snapshot {
            payload = payload.clone();
        }

        /**
         * The payload bytes.
         *
         * @return a copy — callers cannot mutate the snapshot
         */
        @Override
        public byte[] payload() {
            return payload.clone();
        }

        // A record's generated equals/hashCode compare arrays by identity, so two snapshots of
        // the same frame would not be equal. That is a trap in tests and in any deduplication
        // built on these; compare by content instead.

        @Override
        public boolean equals(Object o) {
            return o instanceof Snapshot other
                    && version == other.version
                    && sequence == other.sequence
                    && systemId == other.systemId
                    && componentId == other.componentId
                    && messageId == other.messageId
                    && incompatibleFlags == other.incompatibleFlags
                    && compatibleFlags == other.compatibleFlags
                    && Arrays.equals(payload, other.payload);
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(
                            version,
                            sequence,
                            systemId,
                            componentId,
                            messageId,
                            incompatibleFlags,
                            compatibleFlags);
            return 31 * result + Arrays.hashCode(payload);
        }
    }
}
