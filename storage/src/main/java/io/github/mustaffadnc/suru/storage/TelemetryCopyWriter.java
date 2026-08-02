package io.github.mustaffadnc.suru.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/**
 * Writes measurements in bulk.
 *
 * <p><b>Why {@code COPY} and not {@code INSERT}.</b> Every {@code INSERT} is parsed, planned and
 * round-tripped; at telemetry volumes that overhead dwarfs the data. {@code COPY} streams rows into
 * the server in one statement with no per-row planning, and the difference is not marginal — see
 * {@code docs/benchmarks.md} for the measured ratio on this schema. Batched multi-row inserts close
 * some of the gap and are kept here for exactly that comparison.
 *
 * <p><b>Duplicates are tolerated rather than prevented.</b> Delivery from Kafka is at-least-once:
 * the database transaction commits before the consumer's offsets do, so a crash between those two
 * points replays a batch. Preventing that would need a unique constraint and
 * {@code ON CONFLICT DO NOTHING}, which {@code COPY} cannot use — it would mean staging into a
 * temporary table and inserting across, paying that cost on every batch to avoid a rare one.
 *
 * <p>It is tolerable because a replayed row is byte-identical to the one already stored: same
 * timestamp, same device, same metric, same value. {@code min}, {@code max} and {@code avg} over a
 * bucket are unchanged by repeating a value that is already in it. Only {@code samples} — the count
 * — moves, and it is a diagnostic rather than a measurement. Losing rows would be a correctness
 * problem; counting a handful twice after a crash is not.
 */
public final class TelemetryCopyWriter {

    private static final String COPY_SQL =
            "COPY telemetry (time, tenant_id, device_id, metric, value) FROM STDIN";

    private static final String INSERT_SQL =
            "INSERT INTO telemetry (time, tenant_id, device_id, metric, value) VALUES (?, ?, ?, ?, ?)";

    private final DataSource dataSource;

    /**
     * Creates a writer.
     *
     * @param dataSource the target database
     */
    public TelemetryCopyWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Writes a batch with {@code COPY}.
     *
     * @param rows the measurements
     * @return how many rows the server accepted
     * @throws SQLException if the copy fails
     */
    public long write(List<TelemetryRow> rows) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        byte[] payload = encode(rows);
        try (Connection connection = dataSource.getConnection()) {
            CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
            return copyManager.copyIn(COPY_SQL, new ByteArrayInputStream(payload));
        } catch (IOException e) {
            throw new SQLException("failed streaming COPY payload", e);
        }
    }

    /**
     * Writes a batch with batched {@code INSERT}, for comparison against {@link #write}.
     *
     * @param rows the measurements
     * @return how many rows were inserted
     * @throws SQLException if the insert fails
     */
    public long writeWithInsert(List<TelemetryRow> rows) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            connection.setAutoCommit(false);
            for (TelemetryRow row : rows) {
                statement.setObject(1, row.time().atOffset(java.time.ZoneOffset.UTC));
                statement.setString(2, row.tenantId());
                statement.setString(3, row.deviceId());
                statement.setString(4, row.metric());
                statement.setDouble(5, row.value());
                statement.addBatch();
            }
            int[] applied = statement.executeBatch();
            connection.commit();
            return applied.length;
        }
    }

    /**
     * Renders rows in PostgreSQL's {@code COPY} text format.
     *
     * <p>Tab-separated, newline-terminated. Text rather than binary format: binary is a little
     * faster still, but it encodes each type's on-wire representation by hand, and getting a
     * timestamp's epoch base wrong there produces data that is silently off by decades. The text
     * format's cost is visible and its failure modes are loud.
     */
    private static byte[] encode(List<TelemetryRow> rows) {
        StringBuilder out = new StringBuilder(rows.size() * 96);
        for (TelemetryRow row : rows) {
            appendEscaped(out, row.time().toString());
            out.append('\t');
            appendEscaped(out, row.tenantId());
            out.append('\t');
            appendEscaped(out, row.deviceId());
            out.append('\t');
            appendEscaped(out, row.metric());
            out.append('\t');
            out.append(row.value());
            out.append('\n');
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Escapes the characters COPY text format treats as structural.
     *
     * <p>A device id is attacker-influenced in the sense that it derives from what a peer
     * transmitted, so a raw tab or newline in one would otherwise shift every following column.
     */
    private static void appendEscaped(StringBuilder out, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '\t' -> out.append("\\t");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                default -> out.append(c);
            }
        }
    }

    /**
     * Convenience for building a row.
     *
     * @param time sample time
     * @param tenantId owning tenant
     * @param deviceId source device
     * @param metric metric name
     * @param value the value
     * @return the row
     */
    public static TelemetryRow row(
            Instant time, String tenantId, String deviceId, String metric, double value) {
        return new TelemetryRow(time, tenantId, deviceId, metric, value);
    }
}
