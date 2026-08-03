package io.github.mustaffadnc.suru.controlplane.telemetry;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read API over stored telemetry.
 *
 * <p><b>The device id is a query parameter, not a path segment.</b> A device id is
 * {@code link/sysN} — the link that carried it qualified by MAVLink system id, because system ids
 * are not unique across links. That slash makes it two path segments, and Spring rejects an encoded
 * one by default rather than let a path be smuggled through. So it travels as a parameter, which
 * also keeps the id opaque to routing rather than baking its current shape into the URL space.
 *
 * <p><b>Tenancy comes from a header, and that is temporary.</b> {@code X-Tenant-Id} is trusted
 * outright here, which is adequate for development and is not authentication — anyone who can reach
 * the port can name any tenant. Phase 5 replaces it with a claim from a verified token, at which
 * point this parameter disappears. It is called out rather than left to be discovered because a
 * header named like this is easy to mistake for a security boundary.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Telemetry", description = "Stored telemetry queries")
public class TelemetryController {

    private static final Duration DEFAULT_WINDOW = Duration.ofHours(24);

    private final TelemetryQueryService service;

    /**
     * Creates the controller.
     *
     * @param service the query service
     */
    public TelemetryController(TelemetryQueryService service) {
        this.service = service;
    }

    /**
     * Reads a downsampled series for one metric.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param metric the metric
     * @param from inclusive start
     * @param to exclusive end
     * @param maxPoints how many points to return at most
     * @return the series
     */
    @GetMapping("/telemetry")
    @Operation(
            summary = "Read a downsampled series",
            description =
                    "Chooses the coarsest stored resolution that still resolves the requested "
                            + "bucket, and reports which one answered in the response.")
    public TelemetrySeries series(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam("device") String deviceId,
            @RequestParam("metric") String metric,
            @Parameter(description = "ISO-8601 instant, inclusive")
                    @RequestParam("from")
                    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
                    Instant from,
            @Parameter(description = "ISO-8601 instant, exclusive")
                    @RequestParam("to")
                    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
                    Instant to,
            @RequestParam(name = "maxPoints", defaultValue = "500") int maxPoints) {
        return service.series(tenantId, deviceId, metric, from, to, maxPoints);
    }

    /**
     * The latest value of every metric a device has reported.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param windowHours how far back to look
     * @return metric name to value
     */
    @GetMapping("/devices/latest")
    @Operation(summary = "Latest value of every metric a device has reported")
    public Map<String, Double> latest(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam("device") String deviceId,
            @RequestParam(name = "windowHours", defaultValue = "24") long windowHours) {
        return service.latest(tenantId, deviceId, Duration.ofHours(windowHours));
    }

    /**
     * Devices that have reported recently.
     *
     * @param tenantId owning tenant
     * @param windowHours how far back to look
     * @return device ids
     */
    @GetMapping("/devices")
    @Operation(summary = "Devices seen within a window")
    public List<String> devices(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(name = "windowHours", defaultValue = "24") long windowHours) {
        return service.devices(tenantId, Duration.ofHours(windowHours));
    }

    /**
     * Metrics a device has reported recently.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param windowHours how far back to look
     * @return metric names
     */
    @GetMapping("/devices/metrics")
    @Operation(summary = "Metrics a device has reported within a window")
    public List<String> metrics(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam("device") String deviceId,
            @RequestParam(name = "windowHours", defaultValue = "24") long windowHours) {
        return service.metrics(tenantId, deviceId, Duration.ofHours(windowHours));
    }

    /**
     * Turns an invalid range into a 400 rather than a 500.
     *
     * @param e the failure
     * @return the response
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    /**
     * The default lookback used where a caller does not give one.
     *
     * @return the window
     */
    public static Duration defaultWindow() {
        return DEFAULT_WINDOW;
    }
}
