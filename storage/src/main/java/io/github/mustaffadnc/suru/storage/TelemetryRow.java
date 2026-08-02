package io.github.mustaffadnc.suru.storage;

import java.time.Instant;

/**
 * One measurement, ready to be written.
 *
 * @param time when the sample was taken
 * @param tenantId owning tenant
 * @param deviceId the device it came from
 * @param metric metric name, e.g. {@code position.latitude_deg}
 * @param value the value in its documented unit
 */
public record TelemetryRow(
        Instant time, String tenantId, String deviceId, String metric, double value) {}
