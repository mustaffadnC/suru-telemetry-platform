package io.github.mustaffadnc.suru.protocol.mavlink;

/**
 * Link quality counters from a {@link MavlinkDecoder}.
 *
 * <p>The three failure counters are kept apart on purpose, because they mean different things and
 * a platform that lumps them together cannot tell a broken radio from a configuration mistake:
 *
 * <ul>
 *   <li>{@code checksumErrors} — real corruption; the bytes framed correctly but the checksum
 *       disagreed.
 *   <li>{@code unknownMessages} — well-formed frames carrying message ids outside the dialect.
 *       Nothing is wrong with the link; the decoder simply cannot verify them.
 *   <li>{@code resyncBytes} — bytes discarded while hunting for framing. A boot banner or a
 *       mid-stream join produces these and neither is a fault.
 * </ul>
 *
 * @param framesDecoded frames that passed their checksum
 * @param checksumErrors frames that framed but failed their checksum
 * @param unknownMessages frames skipped because the dialect does not define the message
 * @param resyncBytes bytes discarded while searching for frame alignment
 * @param framesLost frames inferred missing from per-endpoint sequence gaps
 * @param signedFrames decoded frames that carried a v2 signature block
 * @param v1Frames decoded frames that used the v1 framing
 * @param endpointsSeen distinct (system, component) pairs observed
 */
public record MavlinkStats(
        long framesDecoded,
        long checksumErrors,
        long unknownMessages,
        long resyncBytes,
        long framesLost,
        long signedFrames,
        long v1Frames,
        int endpointsSeen) {

    /**
     * Fraction of frames that were expected but never arrived.
     *
     * @return {@code 0.0..1.0}, or {@code 0.0} when nothing has been decoded yet
     */
    public double lossRatio() {
        long expected = framesDecoded + framesLost;
        return expected == 0 ? 0.0 : (double) framesLost / expected;
    }

    @Override
    public String toString() {
        return "frames=%d lost=%d (%.2f%%) crcErrors=%d unknown=%d resyncBytes=%d endpoints=%d"
                .formatted(
                        framesDecoded,
                        framesLost,
                        lossRatio() * 100.0,
                        checksumErrors,
                        unknownMessages,
                        resyncBytes,
                        endpointsSeen);
    }
}
