package io.github.mustaffadnc.suru.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Crc16Test {

    /** The standard check input of the CRC catalogue. */
    private static final byte[] CHECK_INPUT = "123456789".getBytes(StandardCharsets.US_ASCII);

    @Test
    @DisplayName("CCITT-FALSE produces the catalogue check value (ÇARGE frames)")
    void ccittFalseCheckValue() {
        assertThat(Crc16.ccittFalse(CHECK_INPUT)).isEqualTo(0x29B1);
    }

    @Test
    @DisplayName("MCRF4XX produces the catalogue check value (MAVLink v2)")
    void mcrf4xxCheckValue() {
        assertThat(Crc16.mcrf4xx(CHECK_INPUT)).isEqualTo(0x6F91);
    }

    @Test
    @DisplayName("MAVLink is NOT CRC-16/X-25 — the difference is exactly the final XOR 0xFFFF")
    void mavlinkIsNotX25() {
        int mcrf4xx = Crc16.mcrf4xx(CHECK_INPUT);
        int x25WouldBe = mcrf4xx ^ 0xFFFF;

        // X-25's catalogue check value is 0x906E. Picking the wrong variant makes the
        // parser report every packet as corrupt; this test locks that regression out.
        assertThat(x25WouldBe).isEqualTo(0x906E);
        assertThat(mcrf4xx).isNotEqualTo(x25WouldBe);
    }

    @Test
    @DisplayName("The two variants disagree on the same input — they are not interchangeable")
    void variantsAreNotInterchangeable() {
        assertThat(Crc16.ccittFalse(CHECK_INPUT)).isNotEqualTo(Crc16.mcrf4xx(CHECK_INPUT));
    }

    @Test
    @DisplayName("Empty input leaves the seed untouched")
    void emptyInputYieldsInit() {
        byte[] empty = new byte[0];
        assertThat(Crc16.ccittFalse(empty)).isEqualTo(Crc16.INIT);
        assertThat(Crc16.mcrf4xx(empty)).isEqualTo(Crc16.INIT);
    }

    @Test
    @DisplayName("Chunked feed equals whole feed — precondition for the streaming codec")
    void chunkedFeedEqualsWholeFeed() {
        // Same acceptance criterion as in Kerkenez GCS: frame boundaries do not line up
        // with TCP segments, so the codec must produce the same result under any split.
        RandomGenerator rng = RandomGenerator.getDefault();
        byte[] data = new byte[1024];
        rng.nextBytes(data);

        int whole = Crc16.ccittFalse(data);
        int wholeMav = Crc16.mcrf4xx(data);

        for (int chunk : new int[] {1, 3, 7, 64, 511, 1023}) {
            int streamed = Crc16.INIT;
            int streamedMav = Crc16.INIT;
            for (int off = 0; off < data.length; off += chunk) {
                int len = Math.min(chunk, data.length - off);
                streamed = Crc16.ccittFalse(streamed, data, off, len);
                streamedMav = Crc16.mcrf4xx(streamedMav, data, off, len);
            }
            assertThat(streamed).as("CCITT-FALSE, %d-byte chunks", chunk).isEqualTo(whole);
            assertThat(streamedMav).as("MCRF4XX, %d-byte chunks", chunk).isEqualTo(wholeMav);
        }
    }

    @Test
    @DisplayName("Results always stay within 16 bits")
    void resultStaysWithin16Bits() {
        RandomGenerator rng = RandomGenerator.getDefault();
        byte[] data = new byte[256];
        for (int i = 0; i < 500; i++) {
            rng.nextBytes(data);
            assertThat(Crc16.ccittFalse(data)).isBetween(0, 0xFFFF);
            assertThat(Crc16.mcrf4xx(data)).isBetween(0, 0xFFFF);
        }
    }

    @Test
    @DisplayName("Table-driven and bitwise implementations agree on every single byte value")
    void tableAgreesWithBitwiseOnAllBytes() {
        // The table is the optimisation; the bitwise loop is the definition. Checking every
        // one of the 256 possible bytes from every one of 256 sampled seeds covers the whole
        // state transition an incremental CRC can make.
        for (int seed = 0; seed < 0x10000; seed += 257) {
            for (int b = 0; b < 256; b++) {
                byte value = (byte) b;
                assertThat(Crc16.mcrf4xxFast(seed, new byte[] {value}, 0, 1))
                        .as("MCRF4XX seed=0x%04X byte=0x%02X", seed, b)
                        .isEqualTo(Crc16.mcrf4xx(seed, new byte[] {value}, 0, 1));
                assertThat(Crc16.ccittFalseFast(seed, new byte[] {value}, 0, 1))
                        .as("CCITT-FALSE seed=0x%04X byte=0x%02X", seed, b)
                        .isEqualTo(Crc16.ccittFalse(seed, new byte[] {value}, 0, 1));
            }
        }
    }

    @Test
    @DisplayName("Table-driven implementations produce the catalogue check values too")
    void tableProducesCheckValues() {
        assertThat(Crc16.ccittFalseFast(CHECK_INPUT)).isEqualTo(0x29B1);
        assertThat(Crc16.mcrf4xxFast(CHECK_INPUT)).isEqualTo(0x6F91);
    }

    @Test
    @DisplayName("Table-driven and bitwise agree on random buffers of every length")
    void tableAgreesWithBitwiseOnRandomBuffers() {
        RandomGenerator rng = RandomGenerator.of("L64X128MixRandom");
        for (int length = 0; length <= 300; length++) {
            byte[] data = new byte[length];
            rng.nextBytes(data);
            assertThat(Crc16.mcrf4xxFast(data)).as("MCRF4XX len=%d", length).isEqualTo(Crc16.mcrf4xx(data));
            assertThat(Crc16.ccittFalseFast(data))
                    .as("CCITT-FALSE len=%d", length)
                    .isEqualTo(Crc16.ccittFalse(data));
        }
    }

    @Test
    @DisplayName("Any single-bit corruption changes the checksum")
    void singleBitFlipChangesChecksum() {
        RandomGenerator rng = RandomGenerator.getDefault();
        byte[] data = new byte[128];
        rng.nextBytes(data);
        int original = Crc16.ccittFalse(data);
        int originalMav = Crc16.mcrf4xx(data);

        for (int i = 0; i < data.length; i++) {
            for (int bit = 0; bit < 8; bit++) {
                data[i] ^= (byte) (1 << bit);
                assertThat(Crc16.ccittFalse(data)).isNotEqualTo(original);
                assertThat(Crc16.mcrf4xx(data)).isNotEqualTo(originalMav);
                data[i] ^= (byte) (1 << bit);
            }
        }
    }
}
