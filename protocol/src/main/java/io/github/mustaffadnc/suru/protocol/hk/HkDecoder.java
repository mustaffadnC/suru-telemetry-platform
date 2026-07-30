package io.github.mustaffadnc.suru.protocol.hk;

import io.github.mustaffadnc.suru.protocol.Crc16;

/**
 * Streaming decoder for the ÇARGE capsule log framing.
 *
 * <p>Frame layout, little-endian throughout:
 *
 * <pre>
 *   'H' 'K' | ver(1) | type(1) | len(1) | payload(len) | crc16(2)
 * </pre>
 *
 * <p>The checksum is CRC-16/CCITT-FALSE over {@code ver..payload} — that is, everything between the
 * magic and the checksum itself — stored little-endian. Note this is <em>not</em> the reflected
 * variant MAVLink uses; see {@link Crc16}.
 *
 * <p><b>Why resync matters here.</b> These frames are written to an SD card on a capsule that is
 * dropped from an aircraft. Power can be cut mid-write, so the tail of a file is routinely torn,
 * and a corrupted region in the middle must not cost the records after it. The decoder therefore
 * never trusts a length field far enough to skip over it: on any failure it advances a single byte
 * and rescans.
 *
 * <p><b>Streaming versus end-of-file.</b> Mid-stream, a frame that runs past the available bytes is
 * simply incomplete and the decoder waits. At the end of a file that same shape is garbage — a
 * false magic, or a torn tail — and must be resynced past. Callers therefore signal the difference
 * explicitly with {@link #endOfStream()}, which is also what makes this decoder agree byte for byte
 * with the reference Python reader shipped with the firmware.
 *
 * <p>Not thread-safe.
 */
public final class HkDecoder {

    /** First magic byte, {@code 'H'}. */
    public static final int MAGIC_0 = 'H';

    /** Second magic byte, {@code 'K'}. */
    public static final int MAGIC_1 = 'K';

    /** The only frame version this decoder accepts. */
    public static final int VERSION = 1;

    /** Magic(2) + ver + type + len + checksum(2): the smallest possible frame. */
    public static final int MIN_FRAME_LENGTH = 7;

    /** Largest frame: header(5) + 255 payload + checksum(2). */
    public static final int MAX_FRAME_LENGTH = 5 + 255 + 2;

    private static final int HEADER_LENGTH = 5;
    private static final int CHECKSUM_LENGTH = 2;

    /** Receives records as they are decoded. */
    @FunctionalInterface
    public interface RecordHandler {
        /**
         * Called once per valid frame.
         *
         * @param record a view owned by the decoder, valid only until this method returns
         */
        void onRecord(HkRecord record);
    }

    private final HkRecord record = new HkRecord();

    private byte[] buffer = new byte[2 * MAX_FRAME_LENGTH];
    private int readPos;
    private int writePos;

    private long framesDecoded;
    private long checksumErrors;
    private long resyncBytes;
    private long tailBytes;

    /**
     * Feeds bytes into the decoder.
     *
     * @param data source array
     * @param offset first byte to read
     * @param length number of bytes to read
     * @param handler invoked once per decoded record
     */
    public void feed(byte[] data, int offset, int length, RecordHandler handler) {
        append(data, offset, length);
        parse(handler, false);
        compact();
    }

    /**
     * Feeds a whole array into the decoder.
     *
     * @param data bytes to decode
     * @param handler invoked once per decoded record
     */
    public void feed(byte[] data, RecordHandler handler) {
        feed(data, 0, data.length, handler);
    }

    /**
     * Declares that no more bytes are coming, and drains what is left.
     *
     * <p>Anything still buffered that cannot form a valid frame is resynced past and finally
     * charged to {@link HkStats#tailBytes()}. Call this after the last {@code feed} of a file.
     *
     * <p>The handler is mandatory rather than optional: records held back mid-stream are recovered
     * here and nowhere else, so a defaulted no-op handler would silently drop them.
     *
     * @param handler invoked for any records recovered from the tail
     */
    public void endOfStream(RecordHandler handler) {
        parse(handler, true);
        tailBytes += writePos - readPos;
        readPos = writePos;
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

    private void parse(RecordHandler handler, boolean atEnd) {
        while (writePos - readPos >= MIN_FRAME_LENGTH) {
            if ((buffer[readPos] & 0xFF) != MAGIC_0
                    || (buffer[readPos + 1] & 0xFF) != MAGIC_1
                    || (buffer[readPos + 2] & 0xFF) != VERSION) {
                readPos++;
                resyncBytes++;
                continue;
            }

            int payloadLength = buffer[readPos + 4] & 0xFF;
            int total = HEADER_LENGTH + payloadLength + CHECKSUM_LENGTH;

            if (writePos - readPos < total) {
                if (!atEnd) {
                    // Mid-stream: the rest of the frame has not arrived yet.
                    return;
                }
                // At end of file this is a torn tail or a false magic, not a frame.
                readPos++;
                resyncBytes++;
                continue;
            }

            int crcCalculated =
                    Crc16.ccittFalse(Crc16.INIT, buffer, readPos + 2, HEADER_LENGTH - 2 + payloadLength);
            int crcStored =
                    (buffer[readPos + HEADER_LENGTH + payloadLength] & 0xFF)
                            | ((buffer[readPos + HEADER_LENGTH + payloadLength + 1] & 0xFF) << 8);

            if (crcCalculated != crcStored) {
                // Counted separately from resyncBytes: corruption and mere misalignment are
                // different diagnoses even though the recovery is identical.
                readPos++;
                checksumErrors++;
                continue;
            }

            framesDecoded++;
            record.set(
                    buffer[readPos + 3] & 0xFF, buffer, readPos + HEADER_LENGTH, payloadLength);
            handler.onRecord(record);
            readPos += total;
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

    /** Clears buffered bytes and statistics. */
    public void reset() {
        readPos = 0;
        writePos = 0;
        framesDecoded = 0;
        checksumErrors = 0;
        resyncBytes = 0;
        tailBytes = 0;
    }

    /**
     * A snapshot of the decoding statistics gathered so far.
     *
     * @return current counters
     */
    public HkStats stats() {
        return new HkStats(framesDecoded, checksumErrors, resyncBytes, tailBytes);
    }

    /**
     * Bytes buffered but not yet formed into a frame.
     *
     * @return number of pending bytes
     */
    public int pendingBytes() {
        return writePos - readPos;
    }
}
