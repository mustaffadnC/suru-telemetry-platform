package io.github.mustaffadnc.suru.protocol.mavlink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The encoder, checked against bytes a real vehicle actually sent.
 *
 * <p>An encoder validated only by feeding its output to the matching decoder proves the two agree,
 * which they would even if both were wrong in the same way. The test that means something is
 * {@link #reEncodingTheSitlStreamReproducesItByte()}: decode 1058 frames captured off a real
 * ArduPilot link, re-encode each from its decoded parts, and require the concatenation to equal the
 * original recording byte for byte. That checks the header layout, the checksum variant, the
 * CRC_EXTRA feed and v2 truncation against ground truth rather than against this codebase's own
 * assumptions.
 */
class MavlinkEncoderTest {

    private static final MavlinkDialect DIALECT = MavlinkDialect.arduPilotMega();

    private static byte[] sitlStream;

    @BeforeAll
    static void loadStream() throws IOException {
        try (InputStream in =
                MavlinkEncoderTest.class.getResourceAsStream("/mavlink/sitl_stream.bin")) {
            if (in == null) {
                throw new IllegalStateException("sitl_stream.bin missing from the test classpath");
            }
            sitlStream = in.readAllBytes();
        }
    }

    /** What the decoder saw, kept so it can be re-encoded after the callback returns. */
    private record Decoded(
            int version, int sequence, int systemId, int componentId, int messageId, byte[] payload) {}

    /** Where one frame sits in the recording. */
    private record Span(int offset, int length) {}

    /**
     * Locates every frame in the recording by walking its framing directly.
     *
     * <p>Deliberately not done with {@link MavlinkDecoder}: the point of the round-trip test is to
     * compare against the original bytes, so the offsets have to come from somewhere other than the
     * code under test. This walk is a few lines of the wire format and nothing else.
     *
     * <p><b>The recording is not a clean frame stream.</b> Past the 107-byte boot banner there are
     * still twelve bytes of console text interleaved between frames — ArduPilot writes to the same
     * link while booting. A test that assumed frames ran back to back would fail here, and the
     * decoder's resync exists precisely because real links look like this.
     */
    private static List<Span> locateFrames() {
        List<Span> spans = new ArrayList<>();
        int offset = 0;
        while (offset < sitlStream.length) {
            int stx = sitlStream[offset] & 0xFF;
            int total;
            if (stx == MavlinkFrame.STX_V2 && offset + 10 <= sitlStream.length) {
                int payloadLength = sitlStream[offset + 1] & 0xFF;
                boolean signed =
                        (sitlStream[offset + 2] & MavlinkFrame.INCOMPAT_FLAG_SIGNED) != 0;
                total = 12 + payloadLength + (signed ? MavlinkFrame.SIGNATURE_LENGTH : 0);
            } else if (stx == MavlinkFrame.STX_V1 && offset + 6 <= sitlStream.length) {
                total = 8 + (sitlStream[offset + 1] & 0xFF);
            } else {
                offset++;
                continue;
            }
            if (offset + total > sitlStream.length) {
                break;
            }
            spans.add(new Span(offset, total));
            offset += total;
        }
        return spans;
    }

    private static Decoded decodeOne(byte[] bytes) {
        List<Decoded> decoded = new ArrayList<>();
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        decoder.feed(
                bytes,
                0,
                bytes.length,
                frame ->
                        decoded.add(
                                new Decoded(
                                        frame.version(),
                                        frame.sequence(),
                                        frame.systemId(),
                                        frame.componentId(),
                                        frame.messageId(),
                                        frame.copyPayload())));
        assertThat(decoded).as("one frame in, one frame out").hasSize(1);
        return decoded.getFirst();
    }

    private static byte[] reEncode(Decoded frame) {
        MavlinkEncoder encoder =
                new MavlinkEncoder(DIALECT, frame.systemId(), frame.componentId());
        return frame.version() == 2
                ? encoder.encodeV2(frame.messageId(), frame.payload(), frame.sequence())
                : encoder.encodeV1(frame.messageId(), frame.payload(), frame.sequence());
    }

    @Test
    @DisplayName("re-encoding every recorded SITL frame reproduces its bytes exactly")
    void reEncodingTheSitlStreamReproducesItByte() {
        List<Span> spans = locateFrames();
        assertThat(spans).as("the recording holds 1058 frames").hasSize(1058);

        int checked = 0;
        for (Span span : spans) {
            byte[] original = new byte[span.length()];
            System.arraycopy(sitlStream, span.offset(), original, 0, span.length());

            byte[] rebuilt = reEncode(decodeOne(original));

            assertThat(rebuilt)
                    .as("frame %d at offset %d", checked, span.offset())
                    .isEqualTo(original);
            checked++;
        }
        assertThat(checked).isEqualTo(1058);
    }

    @Test
    @DisplayName("the recording really does interleave console text between frames")
    void theRecordingIsNotACleanFrameStream() {
        List<Span> spans = locateFrames();

        int framed = spans.stream().mapToInt(Span::length).sum();
        int firstFrameAt = spans.getFirst().offset();

        assertThat(firstFrameAt).as("boot banner ahead of the first frame").isEqualTo(107);
        assertThat(sitlStream.length - framed - firstFrameAt)
                .as("bytes of console output written between frames while booting")
                .isEqualTo(12);
    }

    @Test
    @DisplayName("an encoded frame decodes back to what it was built from")
    void roundTripsThroughTheDecoder() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 42, 7);
        byte[] payload = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};

        byte[] frame = encoder.encodeV2(MavlinkMetrics.MSG_HEARTBEAT, payload);

        List<Decoded> decoded = new ArrayList<>();
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        decoder.feed(
                frame,
                0,
                frame.length,
                f ->
                        decoded.add(
                                new Decoded(
                                        f.version(),
                                        f.sequence(),
                                        f.systemId(),
                                        f.componentId(),
                                        f.messageId(),
                                        f.copyPayload())));

        assertThat(decoded).hasSize(1);
        Decoded only = decoded.getFirst();
        assertThat(only.version()).isEqualTo(2);
        assertThat(only.systemId()).isEqualTo(42);
        assertThat(only.componentId()).isEqualTo(7);
        assertThat(only.messageId()).isEqualTo(MavlinkMetrics.MSG_HEARTBEAT);
        assertThat(only.payload()).isEqualTo(payload);
    }

    @Test
    @DisplayName("the sequence number advances per endpoint and wraps at 256")
    void sequenceWraps() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 1, 1);
        byte[] payload = new byte[] {1};

        assertThat(encoder.nextSequence()).isZero();
        for (int i = 0; i < 255; i++) {
            encoder.encodeV2(MavlinkMetrics.MSG_HEARTBEAT, payload);
        }
        assertThat(encoder.nextSequence()).isEqualTo(255);

        encoder.encodeV2(MavlinkMetrics.MSG_HEARTBEAT, payload);
        assertThat(encoder.nextSequence())
                .as("the receiver's gap detection counts on this wrapping, not overflowing")
                .isZero();
    }

    /**
     * Truncation, including the floor that the recording caught.
     *
     * <p>The first implementation removed trailing zeros all the way to nothing, which is the
     * obvious reading of "trailing zeros are truncated" and is wrong. Eight frames in the recording
     * carry a payload of exactly one zero byte, and none carries a zero-length payload — see
     * {@link #theRealStreamNeverSendsAZeroLengthPayload()}.
     */
    @Test
    @DisplayName("v2 drops trailing zero bytes but never below one")
    void truncatesTrailingZeros() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 1, 1);

        byte[] padded = new byte[] {7, 0, 0, 0, 0, 0, 0, 0, 0};
        byte[] frame = encoder.encodeV2(MavlinkMetrics.MSG_HEARTBEAT, padded, 0);
        assertThat(frame[1]).as("nine bytes in, one byte on the wire").isEqualTo((byte) 1);

        byte[] allZero = new byte[9];
        byte[] floored = encoder.encodeV2(MavlinkMetrics.MSG_HEARTBEAT, allZero, 0);
        assertThat(floored[1])
                .as("an all-zero payload keeps one byte rather than becoming empty")
                .isEqualTo((byte) 1);
        assertThat(floored).hasSize(1 + 9 + 1 + 2);
    }

    @Test
    @DisplayName("no real frame carries a zero-length payload, and eight carry a single zero byte")
    void theRealStreamNeverSendsAZeroLengthPayload() {
        int zeroLength = 0;
        int singleZeroByte = 0;
        for (Span span : locateFrames()) {
            int declared = sitlStream[span.offset() + 1] & 0xFF;
            if (declared == 0) {
                zeroLength++;
            } else if (declared == 1 && sitlStream[span.offset() + 10] == 0) {
                singleZeroByte++;
            }
        }

        assertThat(zeroLength).as("the floor, observed rather than assumed").isZero();
        assertThat(singleZeroByte)
                .as("payloads that truncate to nothing and are still sent as one byte")
                .isEqualTo(8);
    }

    @Test
    @DisplayName("v1 does not truncate")
    void v1KeepsTrailingZeros() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 1, 1);

        byte[] frame = encoder.encodeV1(MavlinkMetrics.MSG_HEARTBEAT, new byte[9], 0);

        assertThat(frame[1])
                .as("truncation arrived with v2; a v1 receiver expects the declared length")
                .isEqualTo((byte) 9);
    }

    @Test
    @DisplayName("a message outside the dialect is rejected rather than sent uncheckable")
    void unknownMessageRejected() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 1, 1);

        assertThatThrownBy(() -> encoder.encodeV2(0xFFFFF, new byte[] {1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not in dialect");
    }

    @Test
    @DisplayName("a v2-only message id cannot be forced into a v1 frame")
    void wideMessageIdRejectedForV1() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 1, 1);
        int wideId = 300; // beyond a byte, and defined in the dialect

        assertThatThrownBy(() -> encoder.encodeV1(wideId, new byte[] {1}, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not fit in a v1 frame");
    }

    @Test
    @DisplayName("out-of-range endpoint and sequence values are rejected")
    void rangeChecks() {
        assertThatThrownBy(() -> new MavlinkEncoder(DIALECT, 256, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("systemId");
        assertThatThrownBy(() -> new MavlinkEncoder(DIALECT, 1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("componentId");
        assertThatThrownBy(
                        () ->
                                new MavlinkEncoder(DIALECT, 1, 1)
                                        .encodeV2(MavlinkMetrics.MSG_HEARTBEAT, new byte[] {1}, 256))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequenceNumber");
    }
}
