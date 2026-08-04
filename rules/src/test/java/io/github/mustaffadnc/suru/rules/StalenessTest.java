package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Detecting silence, which is the one condition arriving data can never trigger. */
class StalenessTest {

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");

    @Test
    @DisplayName("silence past the limit holds, and only just past it")
    void firesAfterTheLimit() {
        Staleness rule = Staleness.after(Duration.ofSeconds(10));
        Observation seen = Observation.empty("acme", "link/sys1", T0);

        assertThat(rule.holds(seen.asOf(T0.plusSeconds(10)), false))
                .as("exactly at the limit is not yet past it")
                .isFalse();
        assertThat(rule.holds(seen.asOf(T0.plusSeconds(10).plusNanos(1)), false)).isTrue();
    }

    @Test
    @DisplayName("an arriving sample clears the silence")
    void dataResetsTheClock() {
        Staleness rule = Staleness.after(Duration.ofSeconds(10));
        Observation stale = Observation.empty("acme", "link/sys1", T0).asOf(T0.plusSeconds(30));
        assertThat(rule.holds(stale, false)).isTrue();

        Observation heard = stale.with("power.battery_v", 12.4, T0.plusSeconds(30));
        assertThat(rule.holds(heard, true)).isFalse();
    }

    @Test
    @DisplayName("an open alert needs a shorter silence to clear than it took to fire")
    void hysteresisIsInTime() {
        Staleness rule = new Staleness(Duration.ofSeconds(10), Duration.ofSeconds(5));
        Observation quiet = Observation.empty("acme", "link/sys1", T0).asOf(T0.plusSeconds(7));

        assertThat(rule.holds(quiet, false))
                .as("7 s of silence would not fire a 10 s rule")
                .isFalse();
        assertThat(rule.holds(quiet, true))
                .as("but does not release an open alert, which needs the gap under 5 s")
                .isTrue();
    }

    @Test
    @DisplayName("silence cannot be observed without the clock moving")
    void requiresTimeToPass() {
        Staleness rule = Staleness.after(Duration.ofSeconds(10));
        Observation fresh = Observation.empty("acme", "link/sys1", T0);

        // The device is silent, but nothing has advanced the evaluation time, so there is no
        // silence to see yet. This is exactly why the topology punctuates on a timer: a processor
        // that only runs on arriving records would sit here forever.
        assertThat(rule.holds(fresh, false)).isFalse();
        assertThat(fresh.silence()).isZero();
    }

    @Test
    @DisplayName("a clearing gap longer than the firing gap is rejected")
    void unclosableRuleRejected() {
        assertThatThrownBy(
                        () -> new Staleness(Duration.ofSeconds(10), Duration.ofSeconds(20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("could never clear");
    }

    @Test
    @DisplayName("a zero or negative limit is rejected")
    void degenerateLimitsRejected() {
        assertThatThrownBy(() -> Staleness.after(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Staleness.after(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
