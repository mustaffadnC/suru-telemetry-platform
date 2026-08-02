package io.github.mustaffadnc.suru.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GatewayMetricsTest {

    @Test
    @DisplayName("Shed counts are tagged by priority, not summed into one meaningless number")
    void shedIsBrokenOutByPriority() {
        AdmissionController admission = new AdmissionController(100);
        GatewayCounters counters = new GatewayCounters();
        MeterRegistry registry = new SimpleMeterRegistry();
        new GatewayMetrics(admission, counters).bindTo(registry);

        // Drive pressure to a level where bulk is shed and nothing else is.
        for (int i = 0; i < 80; i++) {
            admission.tryAdmit(MessagePriority.CRITICAL);
        }
        for (int i = 0; i < 5; i++) {
            admission.tryAdmit(MessagePriority.BULK);
        }

        double bulkShed =
                registry.get("suru.ingest.shed").tag("priority", "bulk").functionCounter().count();
        double highShed =
                registry.get("suru.ingest.shed").tag("priority", "high").functionCounter().count();
        double criticalShed =
                registry.get("suru.ingest.shed")
                        .tag("priority", "critical")
                        .functionCounter()
                        .count();

        assertThat(bulkShed).isEqualTo(5.0);
        assertThat(highShed).isZero();
        // A dashboard summing these would show "5 messages shed" and imply a problem, when the
        // policy discarded exactly what it is designed to discard. The tag is what makes the
        // difference between an alert worth waking someone for and noise.
        assertThat(criticalShed).isZero();
    }

    @Test
    @DisplayName("Pressure and read pauses are exposed, the two leading indicators")
    void exposesLeadingIndicators() {
        AdmissionController admission = new AdmissionController(100);
        GatewayCounters counters = new GatewayCounters();
        MeterRegistry registry = new SimpleMeterRegistry();
        new GatewayMetrics(admission, counters).bindTo(registry);

        for (int i = 0; i < 65; i++) {
            admission.tryAdmit(MessagePriority.CRITICAL);
        }
        counters.readPaused();

        assertThat(registry.get("suru.ingest.pressure").gauge().value()).isEqualTo(0.65);
        assertThat(registry.get("suru.ingest.in_flight").gauge().value()).isEqualTo(65.0);
        assertThat(registry.get("suru.ingest.capacity").gauge().value()).isEqualTo(100.0);
        assertThat(registry.get("suru.ingest.accepted").functionCounter().count()).isEqualTo(65.0);

        // Read pauses rise while nothing has yet been lost — the warning that precedes damage.
        assertThat(registry.get("suru.ingest.read_pauses").functionCounter().count())
                .isEqualTo(1.0);
        assertThat(registry.get("suru.ingest.shed").tag("priority", "bulk").functionCounter().count())
                .isZero();
    }
}
