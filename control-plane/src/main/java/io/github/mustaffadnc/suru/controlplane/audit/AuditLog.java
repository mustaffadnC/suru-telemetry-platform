package io.github.mustaffadnc.suru.controlplane.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.stereotype.Component;

/**
 * Writes to the append-only audit log.
 *
 * <p>The table refuses {@code UPDATE}, {@code DELETE} and {@code TRUNCATE} at the database level
 * (migration {@code V5}), so this class only ever inserts. There is deliberately no method to
 * amend or remove an entry — not because one would fail, though it would, but because the absence
 * of the method is the first thing a reader checks.
 *
 * <h2>Failing to audit is failing</h2>
 *
 * <p>{@link #record(Connection, AuditEntry)} takes a caller-supplied connection so the audit row can
 * join the transaction that performs the action. For a command that means the audit entry and the
 * command are written together or not at all: <b>no command can exist without the record of who
 * asked for it</b>, which is the property an audit log is for and the one a separate insert quietly
 * gives up the moment anything fails between them.
 *
 * <p>The standalone {@link #record(AuditEntry)} is for events with nothing to be atomic with — a
 * refused request, above all, where there is no other write to join.
 */
@Component
public final class AuditLog {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String INSERT =
            """
            INSERT INTO audit_log (tenant_id, actor, action, subject, outcome, detail)
            VALUES (?, ?, ?, ?, ?, ?::jsonb)
            """;

    private final DataSource dataSource;

    /**
     * Creates an audit log.
     *
     * @param dataSource the database
     */
    public AuditLog(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Records an entry on its own connection.
     *
     * @param entry what happened
     * @throws SQLException if the write fails, which callers must not swallow
     */
    public void record(AuditEntry entry) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            record(connection, entry);
        }
    }

    /**
     * Records an entry inside a caller's transaction.
     *
     * @param connection the transaction to join
     * @param entry what happened
     * @throws SQLException if the write fails
     */
    public void record(Connection connection, AuditEntry entry) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
            statement.setString(1, entry.tenantId());
            statement.setString(2, entry.actor());
            statement.setString(3, entry.action());
            statement.setString(4, entry.subject());
            statement.setString(5, entry.outcome().name());
            statement.setString(6, toJson(entry));
            statement.executeUpdate();
        }
    }

    private static String toJson(AuditEntry entry) throws SQLException {
        try {
            return JSON.writeValueAsString(entry.detail());
        } catch (JsonProcessingException e) {
            throw new SQLException("cannot encode audit detail for " + entry.action(), e);
        }
    }
}
