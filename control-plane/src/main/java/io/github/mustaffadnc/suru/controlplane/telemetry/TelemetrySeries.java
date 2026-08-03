package io.github.mustaffadnc.suru.controlplane.telemetry;

import java.time.Instant;
import java.util.List;

/**
 * A downsampled series returned to a caller.
 *
 * <p>{@code resolution} and {@code bucketSeconds} are part of the response rather than hidden
 * implementation. A client plotting this needs to know whether it is looking at every sample or an
 * hourly mean — a spike that survives raw and disappears at hourly resolution is a different fact
 * about the aircraft than a spike that was never there.
 *
 * @param deviceId the device
 * @param metric the metric
 * @param resolution which stored source answered the query
 * @param bucketSeconds width of each returned bucket
 * @param points the series, oldest first
 */
public record TelemetrySeries(
        String deviceId,
        String metric,
        Resolution resolution,
        long bucketSeconds,
        List<Point> points) {

    /**
     * One bucket.
     *
     * @param time bucket start
     * @param avg mean over the bucket, weighted by sample count when read from a rollup
     * @param min minimum in the bucket
     * @param max maximum in the bucket
     * @param samples how many raw samples the bucket represents
     */
    public record Point(Instant time, double avg, double min, double max, long samples) {}
}
