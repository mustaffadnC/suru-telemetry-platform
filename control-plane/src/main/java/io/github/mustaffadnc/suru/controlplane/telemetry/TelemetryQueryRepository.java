package io.github.mustaffadnc.suru.controlplane.telemetry;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Reads telemetry out of TimescaleDB.
 *
 * <p>Relation names are chosen from {@link Resolution}, never taken from a request. They are
 * interpolated into the SQL because a table name cannot be a bind parameter, so the set of possible
 * values is closed by the enum — every other value in these queries is bound.
 */
@Repository
public class TelemetryQueryRepository {

    private final JdbcClient jdbc;

    /**
     * Creates the repository.
     *
     * @param jdbc the client to query through
     */
    public TelemetryQueryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Reads a downsampled series.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param metric the metric
     * @param from inclusive start
     * @param to exclusive end
     * @param source which stored resolution to read
     * @param bucket output bucket width
     * @return the buckets, oldest first
     */
    public List<TelemetrySeries.Point> series(
            String tenantId,
            String deviceId,
            String metric,
            Instant from,
            Instant to,
            Resolution source,
            Duration bucket) {

        String sql =
                source.isRollup()
                        ? """
                        SELECT time_bucket(CAST(:bucket AS interval), %s) AS b,
                               sum(avg_value * samples) / NULLIF(sum(samples), 0) AS avg_value,
                               min(min_value) AS min_value,
                               max(max_value) AS max_value,
                               sum(samples)   AS samples
                        FROM %s
                        WHERE tenant_id = :tenant AND device_id = :device AND metric = :metric
                          AND %s >= :from AND %s < :to
                        GROUP BY b ORDER BY b
                        """
                                .formatted(
                                        source.timeColumn(),
                                        source.table(),
                                        source.timeColumn(),
                                        source.timeColumn())
                        : """
                        SELECT time_bucket(CAST(:bucket AS interval), %s) AS b,
                               avg(value)   AS avg_value,
                               min(value)   AS min_value,
                               max(value)   AS max_value,
                               count(*)     AS samples
                        FROM %s
                        WHERE tenant_id = :tenant AND device_id = :device AND metric = :metric
                          AND %s >= :from AND %s < :to
                        GROUP BY b ORDER BY b
                        """
                                .formatted(
                                        source.timeColumn(),
                                        source.table(),
                                        source.timeColumn(),
                                        source.timeColumn());

        // The mean over a rollup is weighted by sample count rather than averaged directly.
        // Averaging the minute means would only be exact if every minute held the same number
        // of samples, which telemetry does not guarantee — a minute with three samples would
        // count as much as one with six hundred.
        return jdbc.sql(sql)
                .param("bucket", bucket.toSeconds() + " seconds")
                .param("tenant", tenantId)
                .param("device", deviceId)
                .param("metric", metric)
                .param("from", java.sql.Timestamp.from(from))
                .param("to", java.sql.Timestamp.from(to))
                .query(
                        (rs, rowNum) ->
                                new TelemetrySeries.Point(
                                        rs.getTimestamp("b").toInstant(),
                                        rs.getDouble("avg_value"),
                                        rs.getDouble("min_value"),
                                        rs.getDouble("max_value"),
                                        rs.getLong("samples")))
                .list();
    }

    /**
     * The most recent value of every metric a device has reported.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param since only consider samples at or after this instant
     * @return metric name to latest value
     */
    public Map<String, Double> latest(String tenantId, String deviceId, Instant since) {
        // DISTINCT ON with a matching ORDER BY is PostgreSQL's last-value-per-group: it walks
        // the composite index backwards and stops at the first row of each metric, rather than
        // aggregating the whole range and discarding all but one row per group.
        String sql =
                """
                SELECT DISTINCT ON (metric) metric, value
                FROM telemetry
                WHERE tenant_id = :tenant AND device_id = :device AND time >= :since
                ORDER BY metric, time DESC
                """;
        return jdbc.sql(sql)
                .param("tenant", tenantId)
                .param("device", deviceId)
                .param("since", java.sql.Timestamp.from(since))
                .query()
                .listOfRows()
                .stream()
                .collect(
                        java.util.stream.Collectors.toMap(
                                row -> (String) row.get("metric"),
                                row -> ((Number) row.get("value")).doubleValue()));
    }

    /**
     * Devices that have reported within a window.
     *
     * @param tenantId owning tenant
     * @param since only consider samples at or after this instant
     * @return device ids, alphabetical
     */
    public List<String> devices(String tenantId, Instant since) {
        return jdbc.sql(
                        """
                        SELECT DISTINCT device_id
                        FROM telemetry
                        WHERE tenant_id = :tenant AND time >= :since
                        ORDER BY device_id
                        """)
                .param("tenant", tenantId)
                .param("since", java.sql.Timestamp.from(since))
                .query(String.class)
                .list();
    }

    /**
     * Metrics a device has reported within a window.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param since only consider samples at or after this instant
     * @return metric names, alphabetical
     */
    public List<String> metrics(String tenantId, String deviceId, Instant since) {
        return jdbc.sql(
                        """
                        SELECT DISTINCT metric
                        FROM telemetry
                        WHERE tenant_id = :tenant AND device_id = :device AND time >= :since
                        ORDER BY metric
                        """)
                .param("tenant", tenantId)
                .param("device", deviceId)
                .param("since", java.sql.Timestamp.from(since))
                .query(String.class)
                .list();
    }
}
