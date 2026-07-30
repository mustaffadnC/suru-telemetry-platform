package io.github.mustaffadnc.suru.protocol.mavlink;

import io.github.mustaffadnc.suru.protocol.Crc16;

/**
 * Streaming MAVLink v1/v2 frame decoder.
 *
 * <p>Feed it whatever arrives off the link, in whatever sized pieces it arrives in; frames split
 * across calls are reassembled, and anything that is not a frame is skipped a byte at a time until
 * framing locks again. A real link starts with garbage — an ArduPilot board emits its boot banner
 * as plain text before the first frame — so resync is the normal case, not an error path.
 *
 * <p><b>Checksums.</b> A MAVLink checksum covers the frame from the length byte through the
 * payload, and then absorbs one more byte: the message's {@code CRC_EXTRA}, which encodes its field
 * layout. Two peers that disagree about a message definition therefore fail the checksum rather
 * than silently misreading each other. It follows that a decoder cannot validate a message it does
 * not know — see {@link MavlinkDialect}.
 *
 * <p><b>The algorithm is CRC-16/MCRF4XX, not CRC-16/X-25.</b> The reference C function is named
 * {@code crc_accumulate} and seeds from {@code X25_INIT_CRC}, which invites the wrong choice; X-25
 * finishes with an XOR of {@code 0xFFFF} and MAVLink does not. See {@link Crc16}.
 *
 * <p>Not thread-safe: one decoder per link.
 */
public final class MavlinkDecoder {

    /** Largest frame on the wire: STX + 9 header + 255 payload + 2 checksum + 13 signature. */
    public static final int MAX_FRAME_LENGTH = 1 + 9 + 255 + 2 + MavlinkFrame.SIGNATURE_LENGTH;

    private static final int V2_HEADER_LENGTH = 10;
    private static final int V1_HEADER_LENGTH = 6;
    private static final int CHECKSUM_LENGTH = 2;

    /** Receives frames as they are decoded. */
    @FunctionalInterface
    public interface FrameHandler {
        /**
         * Called once per valid frame.
         *
         * @param frame a view owned by the decoder, valid only until this method returns
         */
        void onFrame(MavlinkFrame frame);
    }

    private final MavlinkDialect dialect;
    private final MavlinkFrame frame = new MavlinkFrame();

    private byte[] buffer = new byte[2 * MAX_FRAME_LENGTH];
    private int readPos;
    private int writePos;

    // Sequence tracking is per (systemId, componentId): a link usually carries several
    // endpoints, each with its own counter. Tracking one global counter — as a naive
    // implementation does — turns ordinary interleaving into phantom packet loss.
    private static final int MAX_ENDPOINTS = 32;
    private final int[] endpointKeys = new int[MAX_ENDPOINTS];
    private final int[] endpointLastSeq = new int[MAX_ENDPOINTS];
    private int endpointCount;

    private long framesDecoded;
    private long checksumErrors;
    private long unknownMessages;
    private long resyncBytes;
    private long framesLost;
    private long signedFrames;
    private long v1Frames;

    /**
     * Creates a decoder bound to a dialect.
     *
     * @param dialect message metadata; {@link MavlinkDialect#arduPilotMega()} for ArduPilot links
     */
    public MavlinkDecoder(MavlinkDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Feeds bytes into the decoder.
     *
     * @param data source array
     * @param offset first byte to read
     * @param length number of bytes to read
     * @param handler invoked once per decoded frame
     */
    public void feed(byte[] data, int offset, int length, FrameHandler handler) {
        append(data, offset, length);
        parse(handler, false);
        compact();
    }

    /**
     * Feeds a whole array into the decoder.
     *
     * @param data bytes to decode
     * @param handler invoked once per decoded frame
     */
    public void feed(byte[] data, FrameHandler handler) {
        feed(data, 0, data.length, handler);
    }

    /**
     * Declares that no more bytes are coming, and drains what is left.
     *
     * <p>Mid-stream, a frame that runs past the buffered bytes is merely incomplete and the decoder
     * waits. Once the link is closed that same shape is garbage — a truncated recording, or a false
     * start byte near the end — and is resynced past instead. Call this when the connection drops
     * or a capture file ends.
     *
     * <p>The handler is mandatory rather than optional: a frame held back mid-stream — because a
     * corrupted length field made the decoder wait for bytes that were never coming — is recovered
     * here and nowhere else. An overload that defaulted to discarding those frames existed briefly
     * and lost data in its first use, so callers who genuinely do not want them must say so at the
     * call site.
     *
     * @param handler invoked for any frames recovered from the tail
     */
    public void endOfStream(FrameHandler handler) {
        parse(handler, true);
        compact();
    }

    private void append(byte[] data, int offset, int length) {
        int leftover = writePos - readPos;
        if (leftover + length > buffer.length) {
            byte[] grown = new byte[Math.max(buffer.length * 2, leftover + length)];
            System.arraycopy(buffer, readPos, grown, 0, leftover);
            buffer = grown;
            readPos = 0;
            writePos = leftover;
        }
        System.arraycopy(data, offset, buffer, writePos, length);
        writePos += length;
    }

    private void parse(FrameHandler handler, boolean atEnd) {
        while (readPos < writePos) {
            int stx = buffer[readPos] & 0xFF;

            if (stx == MavlinkFrame.STX_V2) {
                int outcome = tryDecodeV2(handler);
                if (outcome == NEED_MORE && !atEnd) {
                    return;
                }
                if (outcome == CONSUMED) {
                    continue;
                }
            } else if (stx == MavlinkFrame.STX_V1) {
                int outcome = tryDecodeV1(handler);
                if (outcome == NEED_MORE && !atEnd) {
                    return;
                }
                if (outcome == CONSUMED) {
                    continue;
                }
            }

            // Not a frame start, or a frame that failed to validate: give up one byte and
            // rescan. Advancing by one rather than by the claimed length is what lets the
            // decoder recover from a corrupted length field instead of walking off a cliff.
            readPos++;
            resyncBytes++;
        }
    }

    private static final int CONSUMED = 0;
    private static final int NEED_MORE = 1;
    private static final int REJECTED = 2;

    private int tryDecodeV2(FrameHandler handler) {
        int available = writePos - readPos;
        if (available < V2_HEADER_LENGTH) {
            return NEED_MORE;
        }

        int payloadLength = buffer[readPos + 1] & 0xFF;
        int incompatibleFlags = buffer[readPos + 2] & 0xFF;
        int compatibleFlags = buffer[readPos + 3] & 0xFF;
        int signatureLength =
                (incompatibleFlags & MavlinkFrame.INCOMPAT_FLAG_SIGNED) != 0
                        ? MavlinkFrame.SIGNATURE_LENGTH
                        : 0;
        int total = V2_HEADER_LENGTH + payloadLength + CHECKSUM_LENGTH + signatureLength;
        if (available < total) {
            return NEED_MORE;
        }

        int messageId =
                (buffer[readPos + 7] & 0xFF)
                        | ((buffer[readPos + 8] & 0xFF) << 8)
                        | ((buffer[readPos + 9] & 0xFF) << 16);

        int crcExtra = dialect.crcExtra(messageId);
        if (crcExtra == MavlinkDialect.UNKNOWN) {
            // The checksum cannot be computed without CRC_EXTRA, so this frame can be neither
            // accepted nor proven corrupt. It is skipped and counted on its own rather than
            // charged to checksumErrors — conflating the two is what makes a ground station on
            // the wrong dialect report a healthy link as broken.
            unknownMessages++;
            readPos += total;
            return CONSUMED;
        }

        if (!checksumMatches(readPos + 1, 9 + payloadLength, crcExtra)) {
            checksumErrors++;
            return REJECTED;
        }

        int sequence = buffer[readPos + 4] & 0xFF;
        int systemId = buffer[readPos + 5] & 0xFF;
        int componentId = buffer[readPos + 6] & 0xFF;

        if (signatureLength != 0) {
            signedFrames++;
        }
        accept(
                handler,
                2,
                sequence,
                systemId,
                componentId,
                messageId,
                incompatibleFlags,
                compatibleFlags,
                readPos + V2_HEADER_LENGTH,
                payloadLength);
        readPos += total;
        return CONSUMED;
    }

    private int tryDecodeV1(FrameHandler handler) {
        int available = writePos - readPos;
        if (available < V1_HEADER_LENGTH) {
            return NEED_MORE;
        }

        int payloadLength = buffer[readPos + 1] & 0xFF;
        int total = V1_HEADER_LENGTH + payloadLength + CHECKSUM_LENGTH;
        if (available < total) {
            return NEED_MORE;
        }

        int messageId = buffer[readPos + 5] & 0xFF;
        int crcExtra = dialect.crcExtra(messageId);
        if (crcExtra == MavlinkDialect.UNKNOWN) {
            unknownMessages++;
            readPos += total;
            return CONSUMED;
        }

        if (!checksumMatches(readPos + 1, 5 + payloadLength, crcExtra)) {
            checksumErrors++;
            return REJECTED;
        }

        int sequence = buffer[readPos + 2] & 0xFF;
        int systemId = buffer[readPos + 3] & 0xFF;
        int componentId = buffer[readPos + 4] & 0xFF;

        v1Frames++;
        accept(
                handler,
                1,
                sequence,
                systemId,
                componentId,
                messageId,
                0,
                0,
                readPos + V1_HEADER_LENGTH,
                payloadLength);
        readPos += total;
        return CONSUMED;
    }

    /**
     * Checksum region runs from {@code from} for {@code length} bytes, then CRC_EXTRA.
     *
     * <p>Uses the table-driven CRC. Measured, not assumed: checksumming turned out to be
     * essentially the whole cost of decoding, and the table is 3–5× faster than the bitwise loop at
     * these frame sizes. See docs/benchmarks.md.
     */
    private boolean checksumMatches(int from, int length, int crcExtra) {
        int crc = Crc16.mcrf4xxFast(Crc16.INIT, buffer, from, length);
        crc = Crc16.mcrf4xxFastUpdate(crc, (byte) crcExtra);
        int stored = (buffer[from + length] & 0xFF) | ((buffer[from + length + 1] & 0xFF) << 8);
        return crc == stored;
    }

    private void accept(
            FrameHandler handler,
            int version,
            int sequence,
            int systemId,
            int componentId,
            int messageId,
            int incompatibleFlags,
            int compatibleFlags,
            int payloadOffset,
            int payloadLength) {
        framesDecoded++;
        trackSequence(systemId, componentId, sequence);
        frame.set(
                version,
                sequence,
                systemId,
                componentId,
                messageId,
                incompatibleFlags,
                compatibleFlags,
                buffer,
                payloadOffset,
                payloadLength);
        handler.onFrame(frame);
    }

    private void trackSequence(int systemId, int componentId, int sequence) {
        int key = (systemId << 8) | componentId;
        for (int i = 0; i < endpointCount; i++) {
            if (endpointKeys[i] == key) {
                int expected = (endpointLastSeq[i] + 1) & 0xFF;
                if (sequence != expected) {
                    framesLost += (sequence - expected) & 0xFF;
                }
                endpointLastSeq[i] = sequence;
                return;
            }
        }
        if (endpointCount < MAX_ENDPOINTS) {
            // First frame from this endpoint establishes the baseline; no loss is inferred
            // from it, because there is nothing yet to compare against.
            endpointKeys[endpointCount] = key;
            endpointLastSeq[endpointCount] = sequence;
            endpointCount++;
        }
    }

    private void compact() {
        if (readPos == 0) {
            return;
        }
        int leftover = writePos - readPos;
        if (leftover > 0) {
            System.arraycopy(buffer, readPos, buffer, 0, leftover);
        }
        readPos = 0;
        writePos = leftover;
    }

    /** Clears buffered bytes, statistics and sequence baselines. */
    public void reset() {
        readPos = 0;
        writePos = 0;
        endpointCount = 0;
        framesDecoded = 0;
        checksumErrors = 0;
        unknownMessages = 0;
        resyncBytes = 0;
        framesLost = 0;
        signedFrames = 0;
        v1Frames = 0;
    }

    /**
     * A snapshot of the link statistics gathered so far.
     *
     * @return current counters
     */
    public MavlinkStats stats() {
        return new MavlinkStats(
                framesDecoded,
                checksumErrors,
                unknownMessages,
                resyncBytes,
                framesLost,
                signedFrames,
                v1Frames,
                endpointCount);
    }

    /**
     * Bytes buffered but not yet formed into a frame — a partially received frame, essentially.
     *
     * @return number of pending bytes
     */
    public int pendingBytes() {
        return writePos - readPos;
    }

    /**
     * The dialect this decoder validates against.
     *
     * @return the dialect
     */
    public MavlinkDialect dialect() {
        return dialect;
    }
}
