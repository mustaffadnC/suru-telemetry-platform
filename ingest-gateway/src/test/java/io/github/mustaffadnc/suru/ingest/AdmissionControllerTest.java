package io.github.mustaffadnc.suru.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AdmissionControllerTest {

    /** Raises pressure to a known level by admitting traffic that is never shed. */
    private static AdmissionController atPressure(int capacity, double target) {
        AdmissionController controller = new AdmissionController(capacity);
        int toAdmit = (int) Math.round(capacity * target);
        for (int i = 0; i < toAdmit; i++) {
            controller.tryAdmit(MessagePriority.CRITICAL);
        }
        return controller;
    }

    @Test
    @DisplayName("An idle gateway admits every band")
    void idleAdmitsEverything() {
        AdmissionController controller = new AdmissionController(100);
        for (MessagePriority priority : MessagePriority.values()) {
            assertThat(controller.tryAdmit(priority)).as("%s", priority).isTrue();
        }
        assertThat(controller.stats().shedTotal()).isZero();
    }

    @Test
    @DisplayName("Reads pause before anything at all is shed — the lossless remedy comes first")
    void pausingPrecedesShedding() {
        // This is the ordering invariant of the whole policy. Pausing a TCP read loses
        // nothing; shedding always loses something. So there must exist a pressure band where
        // the gateway has stopped reading and is still discarding nothing, and shedding may
        // only begin above it.
        AdmissionController controller = atPressure(100, 0.65);

        assertThat(controller.shouldPauseReading()).isTrue();
        for (MessagePriority priority : MessagePriority.values()) {
            assertThat(priority.shouldShedAt(controller.pressure()))
                    .as("%s must not be shed while pausing is still an untried remedy", priority)
                    .isFalse();
        }
    }

    @Test
    @DisplayName("Bulk diagnostics go first, once pausing has not been enough")
    void shedsBulkFirst() {
        AdmissionController controller = atPressure(100, 0.75);

        assertThat(controller.tryAdmit(MessagePriority.BULK)).isFalse();
        assertThat(controller.tryAdmit(MessagePriority.NORMAL)).isTrue();
        assertThat(controller.tryAdmit(MessagePriority.HIGH)).isTrue();
        assertThat(controller.tryAdmit(MessagePriority.CRITICAL)).isTrue();
    }

    @Test
    @DisplayName("Ordinary telemetry follows")
    void shedsNormalNext() {
        AdmissionController controller = atPressure(100, 0.85);

        assertThat(controller.tryAdmit(MessagePriority.BULK)).isFalse();
        assertThat(controller.tryAdmit(MessagePriority.NORMAL)).isFalse();
        assertThat(controller.tryAdmit(MessagePriority.HIGH)).isTrue();
        assertThat(controller.tryAdmit(MessagePriority.CRITICAL)).isTrue();
    }

    @Test
    @DisplayName("The live picture degrades only in extremis")
    void shedsHighLast() {
        AdmissionController controller = atPressure(100, 0.95);

        assertThat(controller.tryAdmit(MessagePriority.HIGH)).isFalse();
        assertThat(controller.tryAdmit(MessagePriority.CRITICAL)).isTrue();
    }

    @Test
    @DisplayName("Heartbeats survive even past saturation — losing them fakes a lost vehicle")
    void criticalIsNeverShed() {
        // Deliberately pushed well beyond capacity: a heartbeat dropped under load would make
        // the platform declare a healthy aircraft missing, during the very incident causing
        // the load. No pressure level justifies that.
        AdmissionController controller = atPressure(100, 3.0);

        assertThat(controller.pressure()).isGreaterThan(1.0);
        assertThat(controller.tryAdmit(MessagePriority.CRITICAL)).isTrue();
        assertThat(controller.stats().shedCritical()).isZero();
        assertThat(controller.stats().shedOnlyDisposable()).isTrue();
    }

    @Test
    @DisplayName("Releasing capacity lets shed bands recover")
    void releaseRestoresAdmission() {
        AdmissionController controller = atPressure(100, 0.80);
        assertThat(controller.tryAdmit(MessagePriority.BULK)).isFalse();

        for (int i = 0; i < 20; i++) {
            controller.release();
        }

        assertThat(controller.pressure()).isLessThan(0.75);
        assertThat(controller.tryAdmit(MessagePriority.BULK)).isTrue();
    }

    @Test
    @DisplayName("Pause and resume thresholds differ, so reading does not oscillate")
    void watermarksAreHysteretic() {
        AdmissionController controller = atPressure(100, 0.65);
        assertThat(controller.shouldPauseReading()).isTrue();
        assertThat(controller.mayResumeReading()).isFalse();

        // Falling just under the pause threshold must NOT immediately resume: if it did, the
        // gateway would flip the socket's read gate on every message once it settled near the
        // threshold, costing a syscall each way and achieving nothing.
        for (int i = 0; i < 20; i++) {
            controller.release();
        }
        assertThat(controller.pressure()).isEqualTo(0.45);
        assertThat(controller.shouldPauseReading()).isFalse();
        assertThat(controller.mayResumeReading()).isFalse();

        for (int i = 0; i < 20; i++) {
            controller.release();
        }
        assertThat(controller.mayResumeReading()).isTrue();
    }

    @Test
    @DisplayName("Statistics account for every offered message exactly once")
    void statisticsBalance() {
        AdmissionController controller = atPressure(100, 0.80);
        int offeredBefore = 80;

        for (int i = 0; i < 10; i++) {
            controller.tryAdmit(MessagePriority.BULK);
            controller.tryAdmit(MessagePriority.CRITICAL);
        }

        AdmissionStats stats = controller.stats();
        assertThat(stats.accepted() + stats.shedTotal()).isEqualTo(offeredBefore + 20L);
        assertThat(stats.shedBulk()).isEqualTo(10);
        assertThat(stats.shedCritical()).isZero();
        assertThat(stats.peakInFlight()).isEqualTo(stats.inFlight());
        assertThat(stats.shedRatio()).isBetween(0.0, 1.0);
    }

    @Test
    @DisplayName("Capacity must be positive")
    void rejectsNonPositiveCapacity() {
        assertThatThrownBy(() -> new AdmissionController(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capacity");
    }

    @Test
    @DisplayName("Heartbeats and command acks classify as critical, estimator internals as bulk")
    void mavlinkClassification() {
        assertThat(MessagePriority.ofMavlink(0)).isEqualTo(MessagePriority.CRITICAL); // HEARTBEAT
        assertThat(MessagePriority.ofMavlink(1)).isEqualTo(MessagePriority.CRITICAL); // SYS_STATUS
        assertThat(MessagePriority.ofMavlink(77)).isEqualTo(MessagePriority.CRITICAL); // COMMAND_ACK

        assertThat(MessagePriority.ofMavlink(33)).isEqualTo(MessagePriority.HIGH); // GLOBAL_POSITION
        assertThat(MessagePriority.ofMavlink(30)).isEqualTo(MessagePriority.HIGH); // ATTITUDE

        assertThat(MessagePriority.ofMavlink(164)).isEqualTo(MessagePriority.BULK); // SIMSTATE
        assertThat(MessagePriority.ofMavlink(152)).isEqualTo(MessagePriority.BULK); // MEMINFO

        // An id this build does not know is NORMAL, not BULK: treating the unfamiliar as the
        // most disposable would quietly hide a dialect gap.
        assertThat(MessagePriority.ofMavlink(0x7FFFFF)).isEqualTo(MessagePriority.NORMAL);
    }
}
