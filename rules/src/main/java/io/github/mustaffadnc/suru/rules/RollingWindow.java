package io.github.mustaffadnc.suru.rules;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The recent history of one metric, bounded by time.
 *
 * <p>Immutable, so a window can live in a state store and be serialised without a copy-on-read
 * discipline, and so evaluation can never mutate what it is evaluating.
 *
 * <p>Bounded twice over: by {@code span} and by {@code capacity}. The time bound is what the rules
 * mean; the sample bound is what stops a device reporting at 100 Hz from putting thirty thousand
 * samples into a changelog record. When the capacity bites, the oldest samples go first, so the
 * window silently narrows rather than growing without limit — a trade recorded here because a
 * narrowed window makes a slope less representative, not wrong.
 *
 * @param span how far back the window reaches
 * @param capacity the most samples it will hold
 * @param samples the retained samples, oldest first
 */
public record RollingWindow(Duration span, int capacity, List<Sample> samples) {

    /** One reading. */
    public record Sample(Instant at, double value) {}

    /** Copies the sample list so the window cannot change under a reader. */
    public RollingWindow {
        Objects.requireNonNull(span, "span");
        if (span.isNegative() || span.isZero()) {
            throw new IllegalArgumentException("span must be positive");
        }
        if (capacity < 2) {
            throw new IllegalArgumentException("capacity must be at least 2 to describe a trend");
        }
        samples = List.copyOf(samples);
    }

    /**
     * An empty window.
     *
     * @param span how far back it reaches
     * @param capacity the most samples it holds
     * @return the window
     */
    public static RollingWindow of(Duration span, int capacity) {
        return new RollingWindow(span, capacity, List.of());
    }

    /**
     * This window with one sample added and anything outside the bounds dropped.
     *
     * <p>Samples older than the newest sample minus {@code span} are evicted. The reference point
     * is the newest <em>sample</em>, not the wall clock: a window pinned to the wall clock would
     * empty itself while a device is silent, so the moment the device came back its history would
     * be gone and every trend rule would need to fill the window again before it could fire.
     *
     * @param at when the reading was taken
     * @param value the reading
     * @return a new window
     */
    public RollingWindow with(Instant at, double value) {
        List<Sample> next = new ArrayList<>(samples.size() + 1);
        next.addAll(samples);
        next.add(new Sample(at, value));
        // Out-of-order arrival is rare but real; keeping the list sorted keeps the slope honest.
        next.sort((a, b) -> a.at().compareTo(b.at()));

        Instant newest = next.getLast().at();
        Instant cutoff = newest.minus(span);
        next.removeIf(sample -> sample.at().isBefore(cutoff));

        while (next.size() > capacity) {
            next.removeFirst();
        }
        return new RollingWindow(span, capacity, next);
    }

    /**
     * Summarises the window.
     *
     * <p>The slope is a least-squares fit rather than the difference between the first and last
     * samples. The endpoint difference depends entirely on two readings, so one bad sample at
     * either end swings it arbitrarily; the fit uses all of them.
     *
     * <p><b>That is an improvement, not robustness, and the difference matters.</b> A regression
     * weights samples by their distance from the mean time, so an outlier near either end of the
     * window carries high leverage and moves the answer a long way. Measured on a steady 1 %/min
     * discharge with one dropped reading:
     *
     * <pre>
     *   n=4,  outlier at the end     least squares -9.10 /min   endpoint -10.00 /min
     *   n=4,  outlier in the middle  least squares +1.90 /min   endpoint  -1.00 /min
     *   n=11, outlier at the end     least squares -1.91 /min   endpoint  -3.00 /min
     * </pre>
     *
     * With four samples it is barely better than the cheap estimator, and a mid-window outlier
     * makes it report the wrong <em>sign</em>. It only starts earning its keep once the outlier is
     * outnumbered. The defence is therefore sample count, not the estimator: see the minimum-sample
     * gate in {@link DerivedMetrics}. A genuinely robust fit — Theil–Sen, or an explicit outlier
     * rejection pass — is the upgrade if field data shows dropouts still driving false alerts.
     *
     * @return the summary, or {@link WindowStats#EMPTY} when the window is empty
     */
    public WindowStats stats() {
        if (samples.isEmpty()) {
            return WindowStats.EMPTY;
        }
        int n = samples.size();
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        for (Sample sample : samples) {
            min = Math.min(min, sample.value());
            max = Math.max(max, sample.value());
            sum += sample.value();
        }
        double mean = sum / n;

        double squaredError = 0;
        for (Sample sample : samples) {
            double delta = sample.value() - mean;
            squaredError += delta * delta;
        }
        double stddev = Math.sqrt(squaredError / n);

        return new WindowStats(n, min, max, mean, stddev, slopePerMinute(mean));
    }

    /**
     * Least-squares slope, converted to units per minute.
     *
     * <p>Time is measured in seconds from the first sample rather than from the epoch. Epoch
     * seconds are around 1.8×10⁹, and squaring them in the denominator of a regression loses most
     * of a double's precision to cancellation — the classic way this calculation goes quietly
     * wrong.
     */
    private double slopePerMinute(double meanValue) {
        int n = samples.size();
        if (n < 2) {
            return 0;
        }
        Instant origin = samples.getFirst().at();

        double meanTime = 0;
        for (Sample sample : samples) {
            meanTime += secondsSince(origin, sample.at());
        }
        meanTime /= n;

        double covariance = 0;
        double timeVariance = 0;
        for (Sample sample : samples) {
            double dt = secondsSince(origin, sample.at()) - meanTime;
            covariance += dt * (sample.value() - meanValue);
            timeVariance += dt * dt;
        }
        if (timeVariance == 0) {
            // Every sample carries the same timestamp; there is no interval to divide by.
            return 0;
        }
        return covariance / timeVariance * 60.0;
    }

    private static double secondsSince(Instant origin, Instant at) {
        return Duration.between(origin, at).toNanos() / 1_000_000_000.0;
    }
}
