package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The rolling window, its bounds, and the regression under it. */
class RollingWindowTest {

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");

    private static Builder fixture() {
        return new Builder();
    }

    /** Builds a window from (second, value) pairs. */
    private static final class Builder {
        private RollingWindow window = RollingWindow.of(Duration.ofMinutes(10), 64);

        Builder at(long second, double value) {
            window = window.with(T0.plusSeconds(second), value);
            return this;
        }

        RollingWindow build() {
            return window;
        }
    }

    @Test
    @DisplayName("a perfectly linear decline gives exactly its slope")
    void linearDecline() {
        WindowStats stats =
                fixture().at(0, 100).at(60, 90).at(120, 80).at(180, 70).build().stats();

        assertThat(stats.count()).isEqualTo(4);
        assertThat(stats.slopePerMinute()).isCloseTo(-10.0, within(1e-9));
        assertThat(stats.mean()).isCloseTo(85.0, within(1e-9));
        assertThat(stats.standardDeviation()).isCloseTo(11.1803398875, within(1e-9));
        assertThat(stats.min()).isEqualTo(70.0);
        assertThat(stats.max()).isEqualTo(100.0);
    }

    /**
     * One case where least squares beats the endpoint difference — and it is not the general rule.
     *
     * <p>A steady drain with one bad reading at the end. Here the endpoint difference reports
     * −1.67 %/min and would let a battery draining at nearly 5 %/min pass a −4 %/min rule, while
     * the fit reports −4.64. But this is a favourable case, and {@link #leastSquaresIsNotRobust()}
     * is the one that keeps the claim honest. Reference values from an independent implementation.
     */
    @Test
    @DisplayName("least squares beats the endpoint difference on this data")
    void outlierResistance() {
        RollingWindow window =
                fixture()
                        .at(0, 100)
                        .at(60, 90)
                        .at(120, 80)
                        .at(180, 70)
                        .at(240, 60)
                        .at(300, 50)
                        .at(360, 90)
                        .build();

        WindowStats stats = window.stats();
        assertThat(stats.slopePerMinute()).isCloseTo(-4.6428571429, within(1e-9));
        assertThat(stats.mean()).isCloseTo(77.1428571429, within(1e-9));
        assertThat(stats.standardDeviation()).isCloseTo(16.6598625567, within(1e-9));

        double endpointSlope = (90.0 - 100.0) / 360.0 * 60.0;
        assertThat(endpointSlope)
                .as("what the cheap estimator would have said about the same data")
                .isCloseTo(-1.6666666667, within(1e-9));
    }

    /**
     * Pins the limitation, so the estimator is never mistaken for a robust one.
     *
     * <p>A regression weights each sample by its distance from the mean time, so an outlier near
     * either end carries high leverage. On a steady −1 %/min discharge with one dropped reading,
     * four samples are not enough for the fit to help: at the end it lands on −9.10 against the
     * endpoint method's −10.00, and in the middle it reports a <em>rising</em> battery. Only once
     * the outlier is outnumbered does the fit earn its keep.
     *
     * <p>The defence is therefore the sample count, not the estimator — see
     * {@link DerivedMetrics#DEFAULT_MINIMUM_SAMPLES}. This test exists so nobody deletes that gate
     * believing least squares already handles it.
     */
    @Test
    @DisplayName("least squares is not robust: a high-leverage outlier still moves it a long way")
    void leastSquaresIsNotRobust() {
        WindowStats atEnd =
                fixture().at(0, 90).at(60, 89).at(120, 88).at(180, 60).build().stats();
        assertThat(atEnd.slopePerMinute())
                .as("true slope is -1/min; the endpoint method would say -10.00")
                .isCloseTo(-9.1, within(1e-9));

        WindowStats inMiddle =
                fixture().at(0, 90).at(60, 60).at(120, 88).at(180, 87).build().stats();
        assertThat(inMiddle.slopePerMinute())
                .as("a mid-window dropout on four samples reverses the sign entirely")
                .isCloseTo(1.9, within(1e-9));

        RollingWindow outnumbered = RollingWindow.of(Duration.ofMinutes(30), 64);
        for (int minute = 0; minute < 10; minute++) {
            outnumbered = outnumbered.with(T0.plusSeconds(minute * 60L), 90 - minute);
        }
        outnumbered = outnumbered.with(T0.plusSeconds(600), 60);
        assertThat(outnumbered.stats().slopePerMinute())
                .as("eleven samples pull it back within a point of the truth")
                .isCloseTo(-1.9090909091, within(1e-9));
    }

    @Test
    @DisplayName("small jitter around a flat line gives a slope near zero")
    void flatWithJitter() {
        WindowStats stats =
                fixture()
                        .at(0, 50.0)
                        .at(10, 50.4)
                        .at(20, 49.6)
                        .at(30, 50.2)
                        .at(40, 49.8)
                        .build()
                        .stats();

        assertThat(stats.slopePerMinute()).isCloseTo(-0.36, within(1e-9));
        assertThat(stats.mean()).isCloseTo(50.0, within(1e-9));
    }

    @Test
    @DisplayName("samples older than the span are evicted")
    void spanEviction() {
        RollingWindow window = RollingWindow.of(Duration.ofMinutes(2), 64);
        window = window.with(T0, 1.0);
        window = window.with(T0.plusSeconds(60), 2.0);
        window = window.with(T0.plusSeconds(180), 3.0);

        assertThat(window.samples())
                .as("T0 is three minutes behind the newest sample")
                .hasSize(2)
                .extracting(RollingWindow.Sample::value)
                .containsExactly(2.0, 3.0);
    }

    @Test
    @DisplayName("the window narrows rather than growing past its capacity")
    void capacityEviction() {
        RollingWindow window = RollingWindow.of(Duration.ofHours(1), 3);
        for (int i = 0; i < 10; i++) {
            window = window.with(T0.plusSeconds(i), i);
        }

        assertThat(window.samples())
                .hasSize(3)
                .extracting(RollingWindow.Sample::value)
                .containsExactly(7.0, 8.0, 9.0);
    }

    @Test
    @DisplayName("eviction is measured from the newest sample, not the wall clock")
    void spanIsRelativeToTheData() {
        RollingWindow window = RollingWindow.of(Duration.ofMinutes(2), 64);
        window = window.with(T0, 1.0);
        window = window.with(T0.plusSeconds(30), 2.0);

        // Hours later, the device comes back. Its history is still there, so a trend rule can fire
        // on the first new sample instead of refilling the window first.
        window = window.with(T0.plusSeconds(30).plus(Duration.ofHours(6)), 3.0);

        assertThat(window.samples())
                .as("only the new sample survives its own span, but nothing was lost to waiting")
                .hasSize(1);
    }

    @Test
    @DisplayName("an out-of-order sample lands in time order")
    void outOfOrderInsertion() {
        RollingWindow window = RollingWindow.of(Duration.ofMinutes(10), 64);
        window = window.with(T0.plusSeconds(120), 80.0);
        window = window.with(T0, 100.0);
        window = window.with(T0.plusSeconds(60), 90.0);

        assertThat(window.samples())
                .extracting(RollingWindow.Sample::value)
                .containsExactly(100.0, 90.0, 80.0);
        assertThat(window.stats().slopePerMinute())
                .as("the slope must not depend on arrival order")
                .isCloseTo(-10.0, within(1e-9));
    }

    @Test
    @DisplayName("a window with fewer than two samples has no slope")
    void tooFewSamples() {
        assertThat(RollingWindow.of(Duration.ofMinutes(1), 8).stats()).isEqualTo(WindowStats.EMPTY);

        WindowStats single = RollingWindow.of(Duration.ofMinutes(1), 8).with(T0, 42.0).stats();
        assertThat(single.count()).isEqualTo(1);
        assertThat(single.slopePerMinute()).isZero();
        assertThat(single.mean()).isEqualTo(42.0);
    }

    @Test
    @DisplayName("simultaneous samples do not divide by zero")
    void identicalTimestamps() {
        RollingWindow window = RollingWindow.of(Duration.ofMinutes(1), 8);
        window = window.with(T0, 10.0);
        window = window.with(T0, 20.0);

        assertThat(window.stats().slopePerMinute()).isZero();
        assertThat(window.stats().mean()).isEqualTo(15.0);
    }

    /**
     * Guards the reason time is measured from the first sample rather than from the epoch.
     *
     * <p>Epoch seconds are around 1.8×10⁹. Squaring them in the regression's denominator throws
     * away most of a double's significand to cancellation, and the slope comes back visibly wrong
     * while every value in the window is perfectly ordinary. This test would fail on that
     * implementation and pass on this one.
     */
    @Test
    @DisplayName("the regression stays accurate at real epoch timestamps")
    void noPrecisionLossAtEpochScale() {
        RollingWindow window = RollingWindow.of(Duration.ofMinutes(10), 64);
        for (int i = 0; i <= 6; i++) {
            window = window.with(T0.plusSeconds(i * 60L), 100.0 - i * 10.0);
        }

        assertThat(window.stats().slopePerMinute()).isCloseTo(-10.0, within(1e-9));
    }

    @Test
    @DisplayName("degenerate bounds are rejected")
    void invalidBounds() {
        assertThatThrownBy(() -> RollingWindow.of(Duration.ZERO, 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("span must be positive");
        assertThatThrownBy(() -> RollingWindow.of(Duration.ofMinutes(1), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 2");
    }
}
