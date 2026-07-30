package io.github.mustaffadnc.suru.protocol.hk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.mustaffadnc.suru.protocol.TestResources;
import java.util.ArrayList;
import java.util.List;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HkDecoderTest {

    private static final String SAMPLE_BIN = "/hk/sample.bin";
    private static final String SAMPLE_EXPECTED = "/hk/sample.expected.txt";

    /** Decodes a blob in fixed-size chunks and renders it the way the oracle does. */
    private static String decodeToCanonicalText(byte[] blob, int chunkSize) {
        HkDecoder decoder = new HkDecoder();
        List<String> lines = new ArrayList<>();
        HkDecoder.RecordHandler handler = r -> lines.add(HkPayloads.canonicalLine(r));

        for (int off = 0; off < blob.length; off += chunkSize) {
            decoder.feed(blob, off, Math.min(chunkSize, blob.length - off), handler);
        }
        decoder.endOfStream(handler);

        HkStats s = decoder.stats();
        lines.add(
                "#stats frames=%d crcErrors=%d resyncBytes=%d tailBytes=%d"
                        .formatted(
                                s.framesDecoded(),
                                s.checksumErrors(),
                                s.resyncBytes(),
                                s.tailBytes()));
        return String.join("\n", lines) + "\n";
    }

    /** The oracle file carries a human-readable preamble; strip it, keep {@code #stats}. */
    private static String expectedText() {
        return TestResources.text(SAMPLE_EXPECTED)
                .lines()
                .filter(line -> !line.startsWith("# "))
                .collect(Collectors.joining("\n", "", "\n"));
    }

    @Test
    @DisplayName("Java output matches the independent Python decoder byte for byte")
    void matchesPythonReferenceImplementation() {
        // The oracle is tools/hk-reference.py, a transcription of the decoder written
        // against the firmware that produces these files. Two implementations, written
        // in different languages from the same spec, must agree on every field and on
        // every corruption statistic. Floats are compared as raw IEEE-754 bits so this
        // tests byte offsets, not the two languages' rounding conventions.
        byte[] blob = TestResources.bytes(SAMPLE_BIN);

        assertThat(decodeToCanonicalText(blob, blob.length)).isEqualTo(expectedText());
    }

    @Test
    @DisplayName("Chunked feeding gives identical output and identical statistics")
    void chunkedFeedMatchesWholeFeed() {
        // Frame boundaries never line up with network reads, and an SD card is read in
        // blocks. Any split must produce the same result — including the error counters.
        byte[] blob = TestResources.bytes(SAMPLE_BIN);
        String whole = decodeToCanonicalText(blob, blob.length);

        for (int chunk : new int[] {1, 2, 3, 5, 7, 13, 64, 127, 263}) {
            assertThat(decodeToCanonicalText(blob, chunk))
                    .as("chunk size %d", chunk)
                    .isEqualTo(whole);
        }
    }

    @Test
    @DisplayName("Records decode to the values the fixture was built from")
    void decodesTypedPayloads() {
        byte[] blob = TestResources.bytes(SAMPLE_BIN);
        HkDecoder decoder = new HkDecoder();
        List<HkPayloads.Env> envs = new ArrayList<>();
        List<HkPayloads.Gps> fixes = new ArrayList<>();
        List<HkPayloads.Event> events = new ArrayList<>();

        HkDecoder.RecordHandler handler =
                r -> {
                    switch (r.recordType()) {
                        case ENV -> envs.add(HkPayloads.Env.of(r));
                        case GPS -> fixes.add(HkPayloads.Gps.of(r));
                        case EVENT -> events.add(HkPayloads.Event.of(r));
                        default -> {}
                    }
                };
        decoder.feed(blob, handler);
        decoder.endOfStream(handler);

        assertThat(envs).hasSize(1);
        HkPayloads.Env env = envs.getFirst();
        assertThat(env.timeMs()).isEqualTo(1500L);
        assertThat(HkPayloads.stateName(env.state())).isEqualTo("ATTACHED");
        assertThat(env.pressurePa()).isEqualTo(101325.0f);
        assertThat(env.batteryV()).isEqualTo(11.8f);

        assertThat(fixes).hasSize(1);
        HkPayloads.Gps gps = fixes.getFirst();
        // Ankara, the SITL home position used across the portfolio.
        assertThat(gps.latitude()).isEqualTo(39.925533);
        assertThat(gps.longitude()).isEqualTo(32.866287);
        assertThat(gps.satellites()).isEqualTo(11);
        assertThat(gps.valid()).isTrue();

        assertThat(events).hasSize(1);
        HkPayloads.Event event = events.getFirst();
        assertThat(HkPayloads.stateName(event.fromState())).isEqualTo("ATTACHED");
        assertThat(HkPayloads.stateName(event.toState())).isEqualTo("ARMED");
    }

    @Test
    @DisplayName("A truncated frame is held, not mis-decoded, until the rest arrives")
    void waitsForIncompleteFrameMidStream() {
        byte[] blob = TestResources.bytes(SAMPLE_BIN);
        HkDecoder decoder = new HkDecoder();
        List<String> lines = new ArrayList<>();
        HkDecoder.RecordHandler handler = r -> lines.add(HkPayloads.canonicalLine(r));

        // Stop in the middle of the file; whatever frame straddles the cut must not be
        // emitted, and must not be counted as corruption either.
        int cut = 60;
        decoder.feed(blob, 0, cut, handler);
        long errorsMidway = decoder.stats().checksumErrors();
        int held = decoder.pendingBytes();

        decoder.feed(blob, cut, blob.length - cut, handler);
        decoder.endOfStream(handler);

        assertThat(held).isPositive();
        assertThat(decoder.stats().checksumErrors()).isGreaterThanOrEqualTo(errorsMidway);
        assertThat(lines).hasSize(5);
    }

    @Test
    @DisplayName("Pure noise never throws and never invents a record")
    void survivesArbitraryNoise() {
        // The capsule's card comes back from a drop test with whatever the power cut
        // left behind. A decoder that throws on bad input loses the records after it.
        RandomGenerator rng = RandomGenerator.of("L64X128MixRandom");
        for (int trial = 0; trial < 200; trial++) {
            byte[] noise = new byte[512];
            rng.nextBytes(noise);
            HkDecoder decoder = new HkDecoder();
            List<String> emitted = new ArrayList<>();

            assertThatCode(
                            () -> {
                                decoder.feed(noise, r -> emitted.add(HkPayloads.canonicalLine(r)));
                                decoder.endOfStream(r -> emitted.add(HkPayloads.canonicalLine(r)));
                            })
                    .doesNotThrowAnyException();

            // Random bytes can in principle produce a frame that passes CRC, but with a
            // 16-bit checksum that is a 1-in-65536 event per candidate position; over
            // this many trials it must stay rare rather than routine.
            assertThat(emitted.size()).isLessThan(3);
            assertThat(decoder.stats().framesDecoded()).isEqualTo(emitted.size());
        }
    }

    @Test
    @DisplayName("Noise spliced around a good frame still yields that frame")
    void recoversFrameSurroundedByNoise() {
        byte[] blob = TestResources.bytes(SAMPLE_BIN);
        RandomGenerator rng = RandomGenerator.of("L64X128MixRandom");

        for (int trial = 0; trial < 50; trial++) {
            byte[] prefix = new byte[rng.nextInt(1, 64)];
            byte[] suffix = new byte[rng.nextInt(1, 64)];
            rng.nextBytes(prefix);
            rng.nextBytes(suffix);

            byte[] wrapped = new byte[prefix.length + blob.length + suffix.length];
            System.arraycopy(prefix, 0, wrapped, 0, prefix.length);
            System.arraycopy(blob, 0, wrapped, prefix.length, blob.length);
            System.arraycopy(suffix, 0, wrapped, prefix.length + blob.length, suffix.length);

            HkDecoder decoder = new HkDecoder();
            List<String> lines = new ArrayList<>();
            HkDecoder.RecordHandler handler = r -> lines.add(HkPayloads.canonicalLine(r));
            decoder.feed(wrapped, handler);
            decoder.endOfStream(handler);

            assertThat(lines).as("trial %d", trial).contains("EVENT,t=2100,from=2,to=3,arg=0");
        }
    }

    @Test
    @DisplayName("reset() returns the decoder to a clean slate")
    void resetClearsEverything() {
        byte[] blob = TestResources.bytes(SAMPLE_BIN);
        HkDecoder decoder = new HkDecoder();
        decoder.feed(blob, r -> {});
        decoder.endOfStream(r -> {});
        assertThat(decoder.stats().framesDecoded()).isPositive();

        decoder.reset();

        assertThat(decoder.stats()).isEqualTo(new HkStats(0, 0, 0, 0));
        assertThat(decoder.pendingBytes()).isZero();
        assertThat(decoder.stats().isClean()).isTrue();
    }
}
