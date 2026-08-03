package io.github.mustaffadnc.suru.controlplane.telemetry;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

/** Turns a request for a time range into a query against the right stored resolution. */
@Service
public class TelemetryQueryService {

    /** Largest point budget a caller may ask for. */
    public static final int MAX_POINTS_LIMIT = 5_000;

    /** Point budget used when the caller does not specify one. */
    public static final int DEFAULT_MAX_POINTS = 500;

    private final TelemetryQueryRepository repository;

    /**
     * Creates the service.
     *
     * @param repository the store to read through
     */
    public TelemetryQueryService(TelemetryQueryRepository repository) {
        this.repository = repository;
    }

    /**
     * Reads a series, choosing the resolution.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param metric the metric
     * @param from inclusive start
     * @param to exclusive end
     * @param maxPoints how many points to return at most
     * @return the series, annotated with the resolution that answered it
     * @throws IllegalArgumentException if the range is empty or inverted
     */
    public TelemetrySeries series(
            String tenantId,
            String deviceId,
            String metric,
            Instant from,
            Instant to,
            int maxPoints) {

        if (!to.isAfter(from)) {
            throw new IllegalArgumentException("'to' must be after 'from'");
        }
        int budget = Math.clamp(maxPoints, 1, MAX_POINTS_LIMIT);
        Duration span = Duration.between(from, to);
        Duration bucket = Resolution.bucketFor(span, budget);
        Resolution source = Resolution.forTargetBucket(bucket);

        List<TelemetrySeries.Point> points =
                repository.series(tenantId, deviceId, metric, from, to, source, bucket);

        return new TelemetrySeries(deviceId, metric, source, bucket.toSeconds(), points);
    }

    /**
     * The latest value of every metric a device has reported recently.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param window how far back to look
     * @return metric name to value
     */
    public Map<String, Double> latest(String tenantId, String deviceId, Duration window) {
        // Bounded on purpose. Without a lower bound the query would scan every chunk the
        // device has ever written looking for a metric it stopped reporting a year ago, and
        // the cost would grow with retention rather than with how much the caller asked for.
        return repository.latest(tenantId, deviceId, Instant.now().minus(window));
    }

    /**
     * Devices seen within a window.
     *
     * @param tenantId owning tenant
     * @param window how far back to look
     * @return device ids
     */
    public List<String> devices(String tenantId, Duration window) {
        return repository.devices(tenantId, Instant.now().minus(window));
    }

    /**
     * Metrics a device has reported within a window.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param window how far back to look
     * @return metric names
     */
    public List<String> metrics(String tenantId, String deviceId, Duration window) {
        return repository.metrics(tenantId, deviceId, Instant.now().minus(window));
    }
}
