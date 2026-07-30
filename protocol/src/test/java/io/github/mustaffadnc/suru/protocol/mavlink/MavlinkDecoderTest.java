package io.github.mustaffadnc.suru.protocol.mavlink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.mustaffadnc.suru.protocol.TestResources;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MavlinkDecoderTest {

    private static final String SITL_BIN = "/mavlink/sitl_stream.bin";
    private static final String SITL_EXPECTED = "/mavlink/sitl_stream.expected.txt";

    private static final MavlinkDialect DIALECT = MavlinkDialect.arduPilotMega();

    /** Renders a frame exactly as tools/mavlink-reference.py does. */
    private static String canonicalLine(MavlinkFrame f) {
        return "v%d,sys=%d,comp=%d,msg=%d,seq=%d,payload=%s"
                .formatted(
                        f.version(),
                        f.systemId(),
                        f.componentId(),
                        f.messageId(),
                        f.sequence(),
                        HexFormat.of().formatHex(f.copyPayload()));
    }

    /**
     * Feeds a whole stream and closes it, routing both phases to the same handler.
     *
     * <p>Frames held back mid-stream surface only at {@code endOfStream}, so a test that passed a
     * handler to {@code feed} and forgot it at the close would quietly miss them.
     */
    private static void feedAll(MavlinkDecoder decoder, byte[] data, MavlinkDecoder.FrameHandler h) {
        decoder.feed(data, h);
        decoder.endOfStream(h);
    }

    private static String decodeToCanonicalText(byte[] blob, int chunkSize) {
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<String> lines = new ArrayList<>();
        MavlinkDecoder.FrameHandler handler = f -> lines.add(canonicalLine(f));

        for (int off = 0; off < blob.length; off += chunkSize) {
            decoder.feed(blob, off, Math.min(chunkSize, blob.length - off), handler);
        }
        decoder.endOfStream(handler);

        MavlinkStats s = decoder.stats();
        lines.add(
                "#stats frames=%d crcErrors=%d unknown=%d resyncBytes=%d lost=%d signed=%d v1=%d endpoints=%d"
                        .formatted(
                                s.framesDecoded(),
                                s.checksumErrors(),
                                s.unknownMessages(),
                                s.resyncBytes(),
                                s.framesLost(),
                                s.signedFrames(),
                                s.v1Frames(),
                                s.endpointsSeen()));
        return String.join("\n", lines) + "\n";
    }

    private static String expectedText() {
        return TestResources.text(SITL_EXPECTED)
                .lines()
                .filter(line -> !line.startsWith("# "))
                .collect(Collectors.joining("\n", "", "\n"));
    }

    @Test
    @DisplayName("A real recorded SITL flight decodes exactly as the reference decoder sees it")
    void matchesReferenceOnRecordedStream() {
        // 36 KB captured off an actual ArduPilot SITL link, replayed against an oracle
        // written separately from the MAVLink specification. Agreement on all 1058 frames
        // and on every statistic is what makes this more than a self-consistency check.
        byte[] blob = TestResources.bytes(SITL_BIN);

        assertThat(decodeToCanonicalText(blob, blob.length)).isEqualTo(expectedText());
    }

    @Test
    @DisplayName("A real stream decodes with zero checksum errors and zero unknown messages")
    void realStreamIsClean() {
        byte[] blob = TestResources.bytes(SITL_BIN);
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        feedAll(decoder, blob, f -> {});

        MavlinkStats stats = decoder.stats();
        assertThat(stats.framesDecoded()).isEqualTo(1058);
        assertThat(stats.checksumErrors()).isZero();
        assertThat(stats.framesLost()).isZero();
        // Zero unknown messages is the evidence that the ardupilotmega dialect was
        // required: on the plain common dialect ArduPilot's own messages would land here,
        // and a decoder that folded them into checksum errors would call this link broken.
        assertThat(stats.unknownMessages()).isZero();
        assertThat(stats.endpointsSeen()).isEqualTo(1);
    }

    @Test
    @DisplayName("The autopilot's boot banner is resynced past, not mistaken for frames")
    void resyncsPastBootBanner() {
        // ArduPilot prints a text banner before it starts framing, so the very first bytes
        // of a real link are never a frame. 119 bytes of it here.
        byte[] blob = TestResources.bytes(SITL_BIN);
        String banner = new String(blob, 0, 119, StandardCharsets.US_ASCII);
        assertThat(banner).startsWith("\n\nInit ArduCopter");

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        feedAll(decoder, blob, f -> {});

        assertThat(decoder.stats().resyncBytes()).isEqualTo(119);
    }

    @Test
    @DisplayName("Chunked feeding gives identical output and identical statistics")
    void chunkedFeedMatchesWholeFeed() {
        byte[] blob = TestResources.bytes(SITL_BIN);
        String whole = decodeToCanonicalText(blob, blob.length);

        // 1 byte at a time is the pathological case; 293 is deliberately coprime with
        // typical frame sizes so splits land mid-header, mid-payload and mid-checksum.
        for (int chunk : new int[] {1, 2, 7, 64, 293, 1500, 8192}) {
            assertThat(decodeToCanonicalText(blob, chunk))
                    .as("chunk size %d", chunk)
                    .isEqualTo(whole);
        }
    }

    @Test
    @DisplayName("An unknown message is counted apart from checksum errors")
    void unknownMessageIsNotACorruptionError() {
        // Message id 0x7FFFFF is not in any dialect. The frame is well formed; the decoder
        // simply cannot verify it, and saying so is different from calling the link corrupt.
        byte[] frame =
                MavlinkTestFrames.v2Raw(0, 1, 1, 0x7FFFFF, new byte[] {1, 2, 3}, 0, false);
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<String> seen = new ArrayList<>();
        feedAll(decoder, frame, f -> seen.add(canonicalLine(f)));

        assertThat(seen).isEmpty();
        assertThat(decoder.stats().unknownMessages()).isEqualTo(1);
        assertThat(decoder.stats().checksumErrors()).isZero();
        assertThat(decoder.stats().resyncBytes()).isZero();
    }

    @Test
    @DisplayName("A corrupted frame is rejected and the next frame still decodes")
    void recoversAfterCorruptedFrame() {
        byte[] good1 = MavlinkTestFrames.v2(DIALECT, 0, 1, 1, 0, new byte[9]);
        byte[] broken = MavlinkTestFrames.v2(DIALECT, 1, 1, 1, 0, new byte[9]);
        broken[12] ^= (byte) 0xFF; // flip a payload byte, leave the checksum stale
        byte[] good2 = MavlinkTestFrames.v2(DIALECT, 2, 1, 1, 0, new byte[9]);

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<Integer> sequences = new ArrayList<>();
        feedAll(
                decoder,
                MavlinkTestFrames.concat(good1, broken, good2),
                f -> sequences.add(f.sequence()));

        assertThat(sequences).containsExactly(0, 2);
        assertThat(decoder.stats().checksumErrors()).isEqualTo(1);
    }

    @Test
    @DisplayName("Sequence gaps are counted per endpoint, so interleaving is not phantom loss")
    void tracksSequencePerEndpoint() {
        // Two components sending on one link, each with its own counter. A decoder that
        // keeps a single global last-sequence sees this as a storm of lost frames.
        byte[] stream =
                MavlinkTestFrames.concat(
                        MavlinkTestFrames.v2(DIALECT, 10, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 200, 1, 190, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 11, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 201, 1, 190, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 12, 1, 1, 0, new byte[9]));

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        feedAll(decoder, stream, f -> {});

        assertThat(decoder.stats().framesDecoded()).isEqualTo(5);
        assertThat(decoder.stats().framesLost()).isZero();
        assertThat(decoder.stats().endpointsSeen()).isEqualTo(2);
    }

    @Test
    @DisplayName("A real sequence gap is measured, and wrap-around is not a gap")
    void countsRealSequenceGaps() {
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        feedAll(
                decoder,
                MavlinkTestFrames.concat(
                        MavlinkTestFrames.v2(DIALECT, 10, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 14, 1, 1, 0, new byte[9])),
                f -> {});
        assertThat(decoder.stats().framesLost()).isEqualTo(3);

        MavlinkDecoder wrapping = new MavlinkDecoder(DIALECT);
        feedAll(
                wrapping,
                MavlinkTestFrames.concat(
                        MavlinkTestFrames.v2(DIALECT, 254, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 255, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 0, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 1, 1, 1, 0, new byte[9])),
                f -> {});
        assertThat(wrapping.stats().framesLost()).isZero();
    }

    @Test
    @DisplayName("v1 and v2 frames decode from the same stream")
    void handlesMixedProtocolVersions() {
        byte[] stream =
                MavlinkTestFrames.concat(
                        MavlinkTestFrames.v2(DIALECT, 0, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v1(DIALECT, 1, 1, 1, 0, new byte[9]),
                        MavlinkTestFrames.v2(DIALECT, 2, 1, 1, 0, new byte[9]));

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<Integer> versions = new ArrayList<>();
        feedAll(decoder, stream, f -> versions.add(f.version()));

        assertThat(versions).containsExactly(2, 1, 2);
        assertThat(decoder.stats().v1Frames()).isEqualTo(1);
    }

    @Test
    @DisplayName("A signed frame is decoded and its signature block skipped")
    void handlesSignedFrames() {
        int heartbeatCrcExtra = DIALECT.crcExtra(0);
        byte[] signed =
                MavlinkTestFrames.v2Raw(7, 1, 1, 0, new byte[9], heartbeatCrcExtra, true);
        byte[] following = MavlinkTestFrames.v2(DIALECT, 8, 1, 1, 0, new byte[9]);

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<Boolean> signedFlags = new ArrayList<>();
        feedAll(
                decoder,
                MavlinkTestFrames.concat(signed, following),
                f -> signedFlags.add(f.isSigned()));

        // The point is that the 13-byte signature is accounted for in the frame length:
        // get that wrong and the *next* frame is lost, which is how the bug shows up.
        assertThat(signedFlags).containsExactly(true, false);
        assertThat(decoder.stats().signedFrames()).isEqualTo(1);
        assertThat(decoder.stats().checksumErrors()).isZero();
    }

    @Test
    @DisplayName("The frame view is only valid during the callback, snapshots outlive it")
    void snapshotSurvivesTheCallback() {
        byte[] stream =
                MavlinkTestFrames.concat(
                        MavlinkTestFrames.v2(DIALECT, 0, 1, 1, 0, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}),
                        MavlinkTestFrames.v2(DIALECT, 1, 1, 1, 0, new byte[9]));

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<MavlinkFrame.Snapshot> snapshots = new ArrayList<>();
        feedAll(decoder, stream, f -> snapshots.add(f.toImmutable()));

        assertThat(snapshots).hasSize(2);
        assertThat(snapshots.getFirst().payload())
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(snapshots.get(1).payload()).containsOnly(0);
        assertThat(snapshots.getFirst()).isNotEqualTo(snapshots.get(1));
    }

    @Test
    @DisplayName("Pure noise never throws and the decoder stays usable afterwards")
    void survivesArbitraryNoise() {
        RandomGenerator rng = RandomGenerator.of("L64X128MixRandom");
        for (int trial = 0; trial < 200; trial++) {
            byte[] noise = new byte[1024];
            rng.nextBytes(noise);
            MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);

            assertThatCode(() -> feedAll(decoder, noise, f -> {})).doesNotThrowAnyException();

            // Still able to decode a real frame after swallowing a kilobyte of garbage.
            List<Integer> seen = new ArrayList<>();
            decoder.feed(
                    MavlinkTestFrames.v2(DIALECT, 42, 1, 1, 0, new byte[9]),
                    f -> seen.add(f.sequence()));
            assertThat(seen).containsExactly(42);
        }
    }

    @Test
    @DisplayName("A corrupted length field cannot make the decoder skip past real frames")
    void survivesCorruptedLengthField() {
        // Advancing by the claimed length after a failure would walk straight over the
        // frames that follow. Advancing one byte at a time is what makes recovery possible.
        byte[] poisoned = MavlinkTestFrames.v2(DIALECT, 0, 1, 1, 0, new byte[9]);
        poisoned[1] = (byte) 255; // claim a 255-byte payload

        byte[] good = MavlinkTestFrames.v2(DIALECT, 1, 1, 1, 0, new byte[9]);

        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        List<Integer> seen = new ArrayList<>();
        feedAll(decoder, MavlinkTestFrames.concat(poisoned, good), f -> seen.add(f.sequence()));

        assertThat(seen).contains(1);
    }
}
