package io.github.mustaffadnc.suru.protocol.hk;

import java.util.Arrays;

/**
 * A decoded ÇARGE log record: a type byte and its payload.
 *
 * <p><b>Lifetime:</b> the instance handed to a {@link HkDecoder.RecordHandler} is a view owned by
 * the decoder, valid only for the duration of the callback. Use {@link #copyPayload()} to keep it.
 *
 * <p>All multi-byte fields are little-endian, matching the Cortex-M4 that wrote them. The readers
 * below take an offset into the payload rather than returning a wrapped buffer, so decoding a
 * record costs no allocation at all.
 */
public final class HkRecord {

    private int type;
    private byte[] buffer = new byte[0];
    private int offset;
    private int length;

    HkRecord() {}

    void set(int type, byte[] buffer, int offset, int length) {
        this.type = type;
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Raw record type byte.
     *
     * @return {@code 0..255}; see {@link HkRecordType}
     */
    public int type() {
        return type;
    }

    /**
     * The record type, if recognised.
     *
     * @return the matching constant, or {@link HkRecordType#UNKNOWN}
     */
    public HkRecordType recordType() {
        return HkRecordType.fromCode(type);
    }

    /**
     * Payload length.
     *
     * @return length in bytes
     */
    public int payloadLength() {
        return length;
    }

    private void checkRange(int index, int width) {
        if (index < 0 || index + width > length) {
            throw new IndexOutOfBoundsException(
                    "reading %d byte(s) at %d exceeds payload length %d"
                            .formatted(width, index, length));
        }
    }

    /**
     * Reads an unsigned 8-bit value.
     *
     * @param index payload offset
     * @return {@code 0..255}
     * @throws IndexOutOfBoundsException if the read runs past the payload
     */
    public int u8(int index) {
        checkRange(index, 1);
        return buffer[offset + index] & 0xFF;
    }

    /**
     * Reads an unsigned little-endian 16-bit value.
     *
     * @param index payload offset
     * @return {@code 0..65535}
     * @throws IndexOutOfBoundsException if the read runs past the payload
     */
    public int u16(int index) {
        checkRange(index, 2);
        int b = offset + index;
        return (buffer[b] & 0xFF) | ((buffer[b + 1] & 0xFF) << 8);
    }

    /**
     * Reads an unsigned little-endian 32-bit value, widened so it stays unsigned.
     *
     * @param index payload offset
     * @return {@code 0..4294967295}
     * @throws IndexOutOfBoundsException if the read runs past the payload
     */
    public long u32(int index) {
        checkRange(index, 4);
        int b = offset + index;
        return ((long) (buffer[b] & 0xFF))
                | ((long) (buffer[b + 1] & 0xFF) << 8)
                | ((long) (buffer[b + 2] & 0xFF) << 16)
                | ((long) (buffer[b + 3] & 0xFF) << 24);
    }

    /**
     * Reads a little-endian IEEE-754 single.
     *
     * @param index payload offset
     * @return the value
     * @throws IndexOutOfBoundsException if the read runs past the payload
     */
    public float f32(int index) {
        return Float.intBitsToFloat((int) u32(index));
    }

    /**
     * Reads a little-endian IEEE-754 double.
     *
     * @param index payload offset
     * @return the value
     * @throws IndexOutOfBoundsException if the read runs past the payload
     */
    public double f64(int index) {
        checkRange(index, 8);
        int b = offset + index;
        long bits = 0;
        for (int i = 7; i >= 0; i--) {
            bits = (bits << 8) | (buffer[b + i] & 0xFF);
        }
        return Double.longBitsToDouble(bits);
    }

    /**
     * Copies the payload out of the decoder's buffer.
     *
     * @return a fresh array of exactly {@link #payloadLength()} bytes
     */
    public byte[] copyPayload() {
        return Arrays.copyOfRange(buffer, offset, offset + length);
    }

    @Override
    public String toString() {
        return "HkRecord[" + recordType() + "(" + type + ") len=" + length + "]";
    }
}
