package io.github.mustaffadnc.suru.controlplane.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ResolutionTest {

    @Test
    @DisplayName("A month at 500 points reads the hourly rollup, not raw samples")
    void longRangeUsesHourlyRollup() {
        Duration span = Duration.ofDays(30);
        Duration bucket = Resolution.bucketFor(span, 500);

        // 30 days over 500 points is a bucket of about 86 minutes. Answering that from raw
        // samples means reading millions of rows to emit 500, with the database doing the work
        // and the display throwing it away.
        assertThat(bucket).isGreaterThan(Duration.ofHours(1));
        assertThat(Resolution.forTargetBucket(bucket)).isEqualTo(Resolution.HOUR);
    }

    @Test
    @DisplayName("A day at 500 points reads the minute rollup")
    void mediumRangeUsesMinuteRollup() {
        Duration bucket = Resolution.bucketFor(Duration.ofDays(1), 500);

        assertThat(bucket).isBetween(Duration.ofMinutes(2), Duration.ofMinutes(3));
        assertThat(Resolution.forTargetBucket(bucket)).isEqualTo(Resolution.MINUTE);
    }

    @Test
    @DisplayName("A few minutes reads raw samples, because the rollups have thrown that detail away")
    void shortRangeUsesRaw() {
        Duration bucket = Resolution.bucketFor(Duration.ofMinutes(5), 500);

        // The rule only goes one way: a query may read finer than it needs and aggregate down,
        // never coarser, because averaged-away detail cannot be recovered.
        assertThat(bucket).isLessThan(Duration.ofMinutes(1));
        assertThat(Resolution.forTargetBucket(bucket)).isEqualTo(Resolution.RAW);
    }

    @Test
    @DisplayName("Exactly one minute and exactly one hour fall on the coarser side")
    void boundariesResolveToTheCoarserSource() {
        assertThat(Resolution.forTargetBucket(Duration.ofMinutes(1))).isEqualTo(Resolution.MINUTE);
        assertThat(Resolution.forTargetBucket(Duration.ofSeconds(59))).isEqualTo(Resolution.RAW);
        assertThat(Resolution.forTargetBucket(Duration.ofHours(1))).isEqualTo(Resolution.HOUR);
        assertThat(Resolution.forTargetBucket(Duration.ofMinutes(59))).isEqualTo(Resolution.MINUTE);
    }

    @Test
    @DisplayName("A bucket is never shorter than a second")
    void bucketHasAFloor() {
        // Sub-second buckets over a wide range are a way to exhaust memory rather than to gain
        // detail, and a span short enough to want them already has few raw samples.
        assertThat(Resolution.bucketFor(Duration.ofSeconds(10), 5_000))
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(Resolution.bucketFor(Duration.ZERO, 500)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("Asking for more points gives finer buckets and can change the source")
    void pointBudgetDrivesResolution() {
        // Two days is 172,800 seconds, so the budget alone decides the source: 20 points is a
        // 2.4-hour bucket, 500 is 5.8 minutes, and 5,000 is 34 seconds — under a minute, so the
        // rollups no longer have the detail and the query drops to raw.
        Duration span = Duration.ofDays(2);

        assertThat(Resolution.forTargetBucket(Resolution.bucketFor(span, 20)))
                .isEqualTo(Resolution.HOUR);
        assertThat(Resolution.forTargetBucket(Resolution.bucketFor(span, 500)))
                .isEqualTo(Resolution.MINUTE);
        assertThat(Resolution.forTargetBucket(Resolution.bucketFor(span, 5_000)))
                .isEqualTo(Resolution.RAW);
    }

    @Test
    @DisplayName("The rollups are marked as pre-aggregated, the raw table is not")
    void rollupsAreDistinguishable() {
        // The query builder branches on this: a rollup's mean has to be weighted by sample
        // count, and averaging pre-computed averages directly would let a minute with three
        // samples count as much as one with six hundred.
        assertThat(Resolution.RAW.isRollup()).isFalse();
        assertThat(Resolution.MINUTE.isRollup()).isTrue();
        assertThat(Resolution.HOUR.isRollup()).isTrue();

        assertThat(Resolution.RAW.timeColumn()).isEqualTo("time");
        assertThat(Resolution.MINUTE.timeColumn()).isEqualTo("bucket");
    }
}
