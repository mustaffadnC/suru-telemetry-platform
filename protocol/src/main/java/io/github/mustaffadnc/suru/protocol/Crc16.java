package io.github.mustaffadnc.suru.protocol;

/**
 * Frame checksums.
 *
 * <p>Both protocols this platform speaks use polynomial {@code 0x1021}, and they are still
 * <b>not the same CRC</b>. This is the trap everyone writing their own codec falls into:
 *
 * <ul>
 *   <li>{@link #ccittFalse(byte[])} — <b>CRC-16/CCITT-FALSE</b>: MSB-first, non-reflected, init
 *       {@code 0xFFFF}, xorout {@code 0x0000}. Used by the ÇARGE capsule log frame
 *       ({@code 'HK'|ver|type|len|payload|crc16}). Firmware counterpart: {@code hk_crc16_ccitt}
 *       in {@code App/common/crc.c} of HavaKarakolu-Firmware.
 *   <li>{@link #mcrf4xx(byte[])} — <b>CRC-16/MCRF4XX</b>: reflected (effective polynomial
 *       {@code 0x8408}), init {@code 0xFFFF}, xorout {@code 0x0000}. Used by MAVLink v2.
 * </ul>
 *
 * <p><b>Why MAVLink is not X-25:</b> the reference C function is named {@code crc_accumulate} and
 * its seed constant is {@code X25_INIT_CRC}, so it is routinely assumed to be CRC-16/X-25. X-25
 * applies a final XOR of {@code 0xFFFF}; MAVLink does not. The difference is exactly that XOR — for
 * {@code "123456789"} MCRF4XX yields {@code 0x6F91} while X-25 yields {@code 0x906E}. A parser
 * built on the wrong variant reports <i>every</i> packet as corrupt.
 *
 * <p>The implementations are deliberately bitwise (no lookup table) so they stay line-by-line
 * comparable with the reference C code. A table-driven variant is added in Phase 1 and compared
 * with JMH.
 */
public final class Crc16 {

    /** Shared seed value of both CCITT-FALSE and MCRF4XX. */
    public static final int INIT = 0xFFFF;

    private static final int POLY_MSB = 0x1021;
    private static final int POLY_LSB = 0x8408;
    private static final int MASK = 0xFFFF;

    private Crc16() {
        throw new AssertionError("utility class");
    }

    /**
     * CRC-16/CCITT-FALSE — ÇARGE capsule frames.
     *
     * @param data bytes to checksum
     * @return checksum in the range {@code 0..0xFFFF}
     */
    public static int ccittFalse(byte[] data) {
        return ccittFalse(INIT, data, 0, data.length);
    }

    /**
     * CRC-16/CCITT-FALSE, streaming variant. To process data arriving in chunks, feed the previous
     * result back in as {@code crc}; pass {@link #INIT} on the first call.
     *
     * @param crc running value to continue from
     * @param data source array
     * @param offset start index
     * @param length number of bytes to process
     * @return updated running value
     */
    public static int ccittFalse(int crc, byte[] data, int offset, int length) {
        int c = crc & MASK;
        for (int i = offset; i < offset + length; i++) {
            c = ccittFalseUpdate(c, data[i]);
        }
        return c;
    }

    /**
     * CRC-16/CCITT-FALSE, single byte.
     *
     * @param crc running value to continue from
     * @param b byte to process
     * @return updated running value
     */
    public static int ccittFalseUpdate(int crc, byte b) {
        int c = (crc ^ ((b & 0xFF) << 8)) & MASK;
        for (int bit = 0; bit < 8; bit++) {
            c = ((c & 0x8000) != 0) ? (((c << 1) ^ POLY_MSB) & MASK) : ((c << 1) & MASK);
        }
        return c;
    }

    /**
     * CRC-16/MCRF4XX — MAVLink v2 frames.
     *
     * @param data bytes to checksum
     * @return checksum in the range {@code 0..0xFFFF}
     */
    public static int mcrf4xx(byte[] data) {
        return mcrf4xx(INIT, data, 0, data.length);
    }

    /**
     * CRC-16/MCRF4XX, streaming variant.
     *
     * @param crc running value to continue from
     * @param data source array
     * @param offset start index
     * @param length number of bytes to process
     * @return updated running value
     */
    public static int mcrf4xx(int crc, byte[] data, int offset, int length) {
        int c = crc & MASK;
        for (int i = offset; i < offset + length; i++) {
            c = mcrf4xxUpdate(c, data[i]);
        }
        return c;
    }

    /**
     * CRC-16/MCRF4XX, single byte.
     *
     * @param crc running value to continue from
     * @param b byte to process
     * @return updated running value
     */
    public static int mcrf4xxUpdate(int crc, byte b) {
        int c = (crc ^ (b & 0xFF)) & MASK;
        for (int bit = 0; bit < 8; bit++) {
            c = ((c & 1) != 0) ? ((c >>> 1) ^ POLY_LSB) : (c >>> 1);
        }
        return c & MASK;
    }
}
