package io.github.mustaffadnc.suru.ingest;

/**
 * Admission control counters.
 *
 * <p>Shed counts are broken out by band rather than totalled, because the bands mean different
 * things operationally. Shedding {@code BULK} is the policy working as designed and needs no
 * attention. A non-zero {@code HIGH} count means the gateway is degrading the live picture and is
 * genuinely under-provisioned. A non-zero {@code CRITICAL} count should be impossible — the policy
 * never sheds that band — so if one ever appears it is a bug, not congestion.
 *
 * @param accepted messages admitted for publication
 * @param shedCritical CRITICAL messages shed — must always be zero
 * @param shedHigh HIGH messages shed
 * @param shedNormal NORMAL messages shed
 * @param shedBulk BULK messages shed
 * @param inFlight publications currently outstanding
 * @param peakInFlight highest in-flight count seen
 * @param capacity in-flight count treated as full pressure
 */
public record AdmissionStats(
        long accepted,
        long shedCritical,
        long shedHigh,
        long shedNormal,
        long shedBulk,
        long inFlight,
        long peakInFlight,
        int capacity) {

    /**
     * Total messages shed across all bands.
     *
     * @return the sum
     */
    public long shedTotal() {
        return shedCritical + shedHigh + shedNormal + shedBulk;
    }

    /**
     * Fraction of offered messages that were shed.
     *
     * @return {@code 0.0..1.0}, or {@code 0.0} when nothing has been offered
     */
    public double shedRatio() {
        long offered = accepted + shedTotal();
        return offered == 0 ? 0.0 : (double) shedTotal() / offered;
    }

    /**
     * Whether the shedding that occurred stayed within the disposable band.
     *
     * @return {@code true} if only BULK was shed
     */
    public boolean shedOnlyDisposable() {
        return shedCritical == 0 && shedHigh == 0 && shedNormal == 0;
    }

    @Override
    public String toString() {
        return "accepted=%d shed=%d (bulk=%d normal=%d high=%d critical=%d) inFlight=%d/%d peak=%d"
                .formatted(
                        accepted,
                        shedTotal(),
                        shedBulk,
                        shedNormal,
                        shedHigh,
                        shedCritical,
                        inFlight,
                        capacity,
                        peakInFlight);
    }
}
