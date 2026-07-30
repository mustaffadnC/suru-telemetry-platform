package io.github.mustaffadnc.suru.protocol.hk;

/**
 * Decoding statistics from an {@link HkDecoder}.
 *
 * <p>These are the numbers that answer "how much of this flight log survived?" after a capsule
 * comes back with a card that lost power mid-write.
 *
 * @param framesDecoded frames that passed their checksum
 * @param checksumErrors frames that framed correctly but failed their checksum
 * @param resyncBytes bytes discarded while hunting for frame alignment
 * @param tailBytes bytes left over at end of stream that could not start a frame
 */
public record HkStats(long framesDecoded, long checksumErrors, long resyncBytes, long tailBytes) {

    /**
     * Whether the stream decoded without a single discarded or corrupted byte.
     *
     * @return {@code true} if nothing was lost
     */
    public boolean isClean() {
        return checksumErrors == 0 && resyncBytes == 0 && tailBytes == 0;
    }

    @Override
    public String toString() {
        return "frames=%d crcErrors=%d resyncBytes=%d tailBytes=%d"
                .formatted(framesDecoded, checksumErrors, resyncBytes, tailBytes);
    }
}
