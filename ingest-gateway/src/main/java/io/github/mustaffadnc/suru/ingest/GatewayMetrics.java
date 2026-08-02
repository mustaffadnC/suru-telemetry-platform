package io.github.mustaffadnc.suru.ingest;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Exposes the gateway's admission and connection counters as metrics.
 *
 * <p>Shed counts carry a {@code priority} tag rather than being summed into one number. A total is
 * almost useless operationally: shedding bulk diagnostics is the policy working as intended and
 * warrants no attention at all, while a single shed message in the {@code high} band means the live
 * picture is degrading. An alert on the sum would either fire constantly or never.
 *
 * <p>{@code suru.ingest.read_pauses} is the metric to watch. It rises before anything is lost —
 * the gateway coping only because it keeps telling senders to wait — whereas every other counter
 * here only moves once damage is already being done.
 */
public final class GatewayMetrics implements MeterBinder {

    private final AdmissionController admission;
    private final GatewayCounters counters;
    private final Tags tags;

    /**
     * Creates a binder for one gateway.
     *
     * @param admission the admission controller to report on
     * @param counters the gateway counters to report on
     * @param tags tags applied to every meter, e.g. the instance id
     */
    public GatewayMetrics(AdmissionController admission, GatewayCounters counters, Tags tags) {
        this.admission = admission;
        this.counters = counters;
        this.tags = tags;
    }

    /**
     * Creates a binder with no extra tags.
     *
     * @param admission the admission controller to report on
     * @param counters the gateway counters to report on
     */
    public GatewayMetrics(AdmissionController admission, GatewayCounters counters) {
        this(admission, counters, Tags.empty());
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        registry.gauge("suru.ingest.pressure", tags, admission, AdmissionController::pressure);
        registry.gauge(
                "suru.ingest.in_flight", tags, admission, a -> a.stats().inFlight());
        registry.gauge(
                "suru.ingest.in_flight.peak", tags, admission, a -> a.stats().peakInFlight());
        registry.gauge("suru.ingest.capacity", tags, admission, AdmissionController::capacity);

        registry.more()
                .counter("suru.ingest.accepted", tags, admission, a -> a.stats().accepted());

        shedCounter(registry, "critical", stats -> stats.shedCritical());
        shedCounter(registry, "high", stats -> stats.shedHigh());
        shedCounter(registry, "normal", stats -> stats.shedNormal());
        shedCounter(registry, "bulk", stats -> stats.shedBulk());

        registry.more()
                .counter("suru.ingest.read_pauses", tags, counters, GatewayCounters::readPauses);
        registry.more()
                .counter(
                        "suru.ingest.publish_failures",
                        tags,
                        counters,
                        GatewayCounters::publishFailures);
        registry.more()
                .counter(
                        "suru.ingest.connections.accepted",
                        tags,
                        counters,
                        GatewayCounters::connectionsAccepted);
        registry.more()
                .counter(
                        "suru.ingest.connections.rejected",
                        tags,
                        counters,
                        GatewayCounters::connectionsRejected);
    }

    private void shedCounter(
            MeterRegistry registry, String priority, java.util.function.ToLongFunction<AdmissionStats> extractor) {
        registry.more()
                .counter(
                        "suru.ingest.shed",
                        tags.and("priority", priority),
                        admission,
                        a -> extractor.applyAsLong(a.stats()));
    }
}
