package io.github.mustaffadnc.suru.protocol.mavlink;

/**
 * Little-endian field access over a MAVLink payload.
 *
 * <p>Exists so field offsets are written once. The gateway reads fields straight out of the
 * decoder's buffer; the storage layer reads them out of a Kafka record's value, long after the
 * frame is gone. Those are different byte sources for identical layouts, and duplicating the
 * offsets for each would guarantee they eventually disagree — which for wire offsets means
 * plausible numbers that are quietly wrong.
 *
 * <p>Reads past the end return zero rather than failing. A MAVLink v2 sender may drop trailing zero
 * bytes, so a payload is routinely shorter than the message's defined length and the missing tail
 * is implicitly zero.
 */
public interface MavlinkPayload {

    /**
     * Reads an unsigned 8-bit field.
     *
     * @param index payload offset
     * @return {@code 0..255}, or {@code 0} past the truncated tail
     */
    int u8(int index);

    /**
     * Reads a signed 8-bit field.
     *
     * @param index payload offset
     * @return the value, or {@code 0} past the truncated tail
     */
    int i8(int index);

    /**
     * Reads an unsigned little-endian 16-bit field.
     *
     * @param index payload offset
     * @return {@code 0..65535}, or {@code 0} past the truncated tail
     */
    int u16(int index);

    /**
     * Reads a signed little-endian 16-bit field.
     *
     * @param index payload offset
     * @return the value, or {@code 0} past the truncated tail
     */
    int i16(int index);

    /**
     * Reads an unsigned little-endian 32-bit field.
     *
     * @param index payload offset
     * @return {@code 0..4294967295}, or {@code 0} past the truncated tail
     */
    long u32(int index);

    /**
     * Reads a signed little-endian 32-bit field.
     *
     * @param index payload offset
     * @return the value, or {@code 0} past the truncated tail
     */
    int i32(int index);

    /**
     * Reads a little-endian IEEE-754 single field.
     *
     * @param index payload offset
     * @return the value, or {@code 0.0f} past the truncated tail
     */
    float f32(int index);

    /**
     * Wraps a payload byte array.
     *
     * @param payload the bytes, not copied
     * @return an accessor over them
     */
    static MavlinkPayload of(byte[] payload) {
        return new ArrayPayload(payload);
    }

    /** Array-backed accessor. */
    final class ArrayPayload implements MavlinkPayload {
        private final byte[] bytes;

        ArrayPayload(byte[] bytes) {
            this.bytes = bytes;
        }

        private int byteOrZero(int index) {
            return index < 0 || index >= bytes.length ? 0 : bytes[index] & 0xFF;
        }

        @Override
        public int u8(int index) {
            return byteOrZero(index);
        }

        @Override
        public int i8(int index) {
            return (byte) byteOrZero(index);
        }

        @Override
        public int u16(int index) {
            return byteOrZero(index) | (byteOrZero(index + 1) << 8);
        }

        @Override
        public int i16(int index) {
            return (short) u16(index);
        }

        @Override
        public long u32(int index) {
            return ((long) byteOrZero(index))
                    | ((long) byteOrZero(index + 1) << 8)
                    | ((long) byteOrZero(index + 2) << 16)
                    | ((long) byteOrZero(index + 3) << 24);
        }

        @Override
        public int i32(int index) {
            return (int) u32(index);
        }

        @Override
        public float f32(int index) {
            return Float.intBitsToFloat((int) u32(index));
        }
    }
}
