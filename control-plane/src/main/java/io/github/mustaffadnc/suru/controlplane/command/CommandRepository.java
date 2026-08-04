package io.github.mustaffadnc.suru.controlplane.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mustaffadnc.suru.controlplane.audit.AuditEntry;
import io.github.mustaffadnc.suru.controlplane.audit.AuditLog;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.stereotype.Component;

/**
 * Commands and their outbox rows, written together or not at all.
 *
 * <h2>Why an outbox</h2>
 *
 * <p>A command has to be recorded in this database <em>and</em> published towards the vehicle, and
 * there is no transaction spanning both. Each ordering fails differently and both failures are
 * real:
 *
 * <ul>
 *   <li>Publish first, then insert: if the insert fails, a vehicle acts on a command this platform
 *       has no record of. Nothing to audit, nothing to match an ACK against, nothing to show the
 *       operator.
 *   <li>Insert first, then publish: if the publish fails, an operator sees an accepted command that
 *       never left the building.
 * </ul>
 *
 * <p>So both writes go into one database transaction — the command row and its outbox row — and a
 * separate relay publishes from the outbox. The database is the source of truth and the topic is a
 * projection of it. Delivery becomes at-least-once, which is exactly why commands carry idempotency
 * keys and why ACKs are matched by command id.
 *
 * <p>Written against JDBC rather than a transaction annotation, because the guarantee is the one
 * thing this class exists for: the two inserts share an explicit connection with autocommit off,
 * and nothing about that depends on proxying, a call being external, or an annotation being read.
 */
// @Component rather than @Repository. @Repository asks Spring to proxy the bean so it can
// translate SQLException into its own DataAccessException hierarchy — which fails outright on a
// final class, and is unwanted anyway: this class throws SQLException deliberately, and callers
// distinguish a constraint violation from a connection failure by reading it.
@Component
public final class CommandRepository {

    private final DataSource dataSource;
    private final AuditLog auditLog;

    /**
     * Creates a repository.
     *
     * @param dataSource the database
     * @param auditLog where the record of who issued what is written, in the same transaction
     */
    public CommandRepository(DataSource dataSource, AuditLog auditLog) {
        this.dataSource = dataSource;
        this.auditLog = auditLog;
    }

    /**
     * The outcome of issuing a command.
     *
     * @param command the command, newly created or the one the key already referred to
     * @param created {@code false} when an existing command was returned instead
     */
    public record Issued(Command command, boolean created) {}

    /**
     * Records a command and its outbox row in one transaction.
     *
     * <p><b>A repeated idempotency key returns the original command rather than issuing a second
     * one.</b> The caller cannot tell a lost response from a lost request, so it retries; for
     * {@code ARM} or {@code TAKEOFF} the difference between a retry and a second command is the
     * difference between one launch and two. The uniqueness is enforced by the database, so two
     * concurrent requests carrying the same key cannot both win — {@code ON CONFLICT DO NOTHING}
     * makes the loser read the winner's row rather than fail.
     *
     * @param tenantId owning tenant
     * @param deviceId the vehicle
     * @param idempotencyKey the caller's key
     * @param type what to do
     * @param params command parameters
     * @param issuedBy who asked
     * @param topic the topic the relay should publish to
     * @param timeout how long to wait for an ACK before declaring the command timed out
     * @return the command, and whether this call created it
     * @throws SQLException if the transaction fails
     */
    public Issued issue(
            String tenantId,
            String deviceId,
            String idempotencyKey,
            CommandType type,
            Map<String, Double> params,
            String issuedBy,
            String topic,
            Duration timeout)
            throws SQLException {

        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        Instant expiresAt = now.plus(timeout);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                Optional<Command> inserted =
                        insertCommand(
                                connection,
                                id,
                                tenantId,
                                deviceId,
                                idempotencyKey,
                                type,
                                params,
                                issuedBy,
                                now,
                                expiresAt);

                if (inserted.isEmpty()) {
                    // The key already exists. Return the original, and write no outbox row: the
                    // first request's row is already there or already published. No audit entry
                    // either — nothing happened, and a log of things that did not happen is a log
                    // an auditor learns to skim.
                    Command existing =
                            findByIdempotencyKey(connection, tenantId, idempotencyKey)
                                    .orElseThrow(
                                            () ->
                                                    new SQLException(
                                                            "idempotency key conflicted but no row found: "
                                                                    + idempotencyKey));
                    connection.commit();
                    return new Issued(existing, false);
                }

                insertOutbox(connection, id, topic, type, params, deviceId, tenantId);

                // Third write, same transaction. No command can exist without the record of who
                // asked for it: a separate insert would give that up the moment anything failed
                // between the two, and it would fail exactly when someone cared.
                auditLog.record(
                        connection,
                        AuditEntry.allowed(
                                tenantId,
                                issuedBy,
                                "command.issue",
                                deviceId,
                                Map.of(
                                        "commandId", id.toString(),
                                        "type", type.name(),
                                        "params", params.toString())));

                connection.commit();
                return new Issued(inserted.get(), true);
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private static Optional<Command> insertCommand(
            Connection connection,
            UUID id,
            String tenantId,
            String deviceId,
            String idempotencyKey,
            CommandType type,
            Map<String, Double> params,
            String issuedBy,
            Instant now,
            Instant expiresAt)
            throws SQLException {

        String sql =
                """
                INSERT INTO command (id, tenant_id, device_id, idempotency_key, command_type,
                                     mav_command_id, params, state, issued_by, created_at,
                                     updated_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, 'PENDING', ?, ?, ?, ?)
                ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, id);
            statement.setString(2, tenantId);
            statement.setString(3, deviceId);
            statement.setString(4, idempotencyKey);
            statement.setString(5, type.name());
            statement.setInt(6, type.mavCommandId());
            statement.setString(7, toJson(params));
            statement.setString(8, issuedBy);
            statement.setTimestamp(9, Timestamp.from(now));
            statement.setTimestamp(10, Timestamp.from(now));
            statement.setTimestamp(11, Timestamp.from(expiresAt));

            if (statement.executeUpdate() == 0) {
                return Optional.empty();
            }
        }
        return Optional.of(
                new Command(
                        id,
                        tenantId,
                        deviceId,
                        idempotencyKey,
                        type,
                        params,
                        CommandState.PENDING,
                        Command.NO_ACK,
                        null,
                        issuedBy,
                        now,
                        expiresAt));
    }

    private static void insertOutbox(
            Connection connection,
            UUID commandId,
            String topic,
            CommandType type,
            Map<String, Double> params,
            String deviceId,
            String tenantId)
            throws SQLException {

        // param1 distinguishes ARM from DISARM, which share a MAV_CMD id, so it is written into
        // the payload rather than left for the relay to re-derive from the command type.
        Map<String, Double> payloadParams = new HashMap<>(params);
        payloadParams.put("param1", (double) type.param1());

        Map<String, Object> payloadFields = new HashMap<>();
        payloadFields.put("commandId", commandId.toString());
        payloadFields.put("tenantId", tenantId);
        payloadFields.put("deviceId", deviceId);
        payloadFields.put("mavCommandId", type.mavCommandId());
        payloadFields.put("params", payloadParams);

        String payload;
        try {
            payload = JSON.writeValueAsString(payloadFields);
        } catch (JsonProcessingException e) {
            throw new SQLException("cannot encode outbox payload", e);
        }

        String sql =
                "INSERT INTO command_outbox (command_id, topic, payload) VALUES (?, ?, ?::jsonb)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, commandId);
            statement.setString(2, topic);
            statement.setString(3, payload);
            statement.executeUpdate();
        }
    }

    /**
     * Finds a command by its identifier, within a tenant.
     *
     * <p>The tenant is part of the lookup rather than checked afterwards. A query that finds the
     * row and then compares tenants has already read another tenant's data, and the version of it
     * that forgets the comparison looks identical in review.
     *
     * @param tenantId owning tenant
     * @param id the command id
     * @return the command, or empty
     * @throws SQLException if the query fails
     */
    public Optional<Command> find(String tenantId, UUID id) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(SELECT_COMMAND + " WHERE tenant_id = ? AND id = ?")) {
            statement.setString(1, tenantId);
            statement.setObject(2, id);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        }
    }

    /**
     * Records the vehicle's answer.
     *
     * <p><b>Only a command still awaiting an ACK is updated.</b> A duplicate COMMAND_ACK — and
     * at-least-once delivery makes duplicates ordinary — must not reopen a settled command or
     * overwrite the first answer with the second.
     *
     * @param commandId the command the vehicle answered
     * @param result the MAVLink result code
     * @param at when the answer arrived
     * @return {@code true} if this call settled the command
     * @throws SQLException if the update fails
     */
    public boolean recordAck(UUID commandId, int result, Instant at) throws SQLException {
        String sql =
                """
                UPDATE command
                   SET state = CASE WHEN ? = 0 THEN 'ACKED' ELSE 'REJECTED' END,
                       ack_result = ?, ack_at = ?, updated_at = ?
                 WHERE id = ? AND state IN ('PENDING', 'SENT')
                """;
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, result);
            statement.setInt(2, result);
            statement.setTimestamp(3, Timestamp.from(at));
            statement.setTimestamp(4, Timestamp.from(at));
            statement.setObject(5, commandId);
            return statement.executeUpdate() == 1;
        }
    }

    /**
     * Records a vehicle's COMMAND_ACK.
     *
     * <p><b>Matched on {@code (tenant, device, MAV_CMD id)}, because that is all the answer
     * carries.</b> COMMAND_ACK has no correlation id — the vehicle never receives this platform's
     * command id and cannot echo it back — so there is nothing else to match on. What makes that
     * sufficient is the partial unique index from {@code V6}: at most one unanswered command per
     * device and MAV_CMD id, so the match is unique whenever it exists.
     *
     * <p>{@code ORDER BY created_at} is belt and braces. The index makes more than one match
     * impossible, and if a future migration ever relaxed it the oldest outstanding command is the
     * one a vehicle answers first.
     *
     * @param tenantId owning tenant
     * @param deviceId the vehicle that answered
     * @param mavCommandId the MAV_CMD id from the ACK
     * @param result the MAVLink result code, zero meaning accepted
     * @param at when the answer arrived
     * @return the command that was settled, or empty when the ACK matched nothing
     * @throws SQLException if the update fails
     */
    public Optional<UUID> recordAckFromVehicle(
            String tenantId, String deviceId, int mavCommandId, int result, Instant at)
            throws SQLException {
        String sql =
                """
                UPDATE command
                   SET state = CASE WHEN ? = 0 THEN 'ACKED' ELSE 'REJECTED' END,
                       ack_result = ?, ack_at = ?, updated_at = ?
                 WHERE id = (
                       SELECT id FROM command
                        WHERE tenant_id = ? AND device_id = ? AND mav_command_id = ?
                          AND state IN ('PENDING', 'SENT')
                        ORDER BY created_at
                        LIMIT 1)
                RETURNING id
                """;
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, result);
            statement.setInt(2, result);
            statement.setTimestamp(3, Timestamp.from(at));
            statement.setTimestamp(4, Timestamp.from(at));
            statement.setString(5, tenantId);
            statement.setString(6, deviceId);
            statement.setInt(7, mavCommandId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next()
                        ? Optional.of(rows.getObject("id", UUID.class))
                        : Optional.empty();
            }
        }
    }

    /**
     * Marks commands whose window has passed as timed out.
     *
     * @param now the current instant
     * @return how many were expired
     * @throws SQLException if the update fails
     */
    public int expireStale(Instant now) throws SQLException {
        String sql =
                """
                UPDATE command SET state = 'TIMED_OUT', updated_at = ?
                 WHERE state IN ('PENDING', 'SENT') AND expires_at < ?
                """;
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(now));
            statement.setTimestamp(2, Timestamp.from(now));
            return statement.executeUpdate();
        }
    }

    /**
     * Commands for a device, newest first.
     *
     * @param tenantId owning tenant
     * @param deviceId the vehicle
     * @param limit how many to return
     * @return the commands
     * @throws SQLException if the query fails
     */
    public List<Command> findByDevice(String tenantId, String deviceId, int limit)
            throws SQLException {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(
                                SELECT_COMMAND
                                        + " WHERE tenant_id = ? AND device_id = ?"
                                        + " ORDER BY created_at DESC LIMIT ?")) {
            statement.setString(1, tenantId);
            statement.setString(2, deviceId);
            statement.setInt(3, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<Command> commands = new ArrayList<>();
                while (rows.next()) {
                    commands.add(read(rows));
                }
                return commands;
            }
        }
    }

    private static Optional<Command> findByIdempotencyKey(
            Connection connection, String tenantId, String key) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        SELECT_COMMAND + " WHERE tenant_id = ? AND idempotency_key = ?")) {
            statement.setString(1, tenantId);
            statement.setString(2, key);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        }
    }

    private static final String SELECT_COMMAND =
            """
            SELECT id, tenant_id, device_id, idempotency_key, command_type, params, state,
                   ack_result, ack_at, issued_by, created_at, expires_at
              FROM command
            """;

    private static Command read(ResultSet rows) throws SQLException {
        int ackResult = rows.getInt("ack_result");
        if (rows.wasNull()) {
            ackResult = Command.NO_ACK;
        }
        Timestamp ackAt = rows.getTimestamp("ack_at");

        return new Command(
                rows.getObject("id", UUID.class),
                rows.getString("tenant_id"),
                rows.getString("device_id"),
                rows.getString("idempotency_key"),
                CommandType.valueOf(rows.getString("command_type")),
                fromJson(rows.getString("params")),
                CommandState.valueOf(rows.getString("state")),
                ackResult,
                ackAt == null ? null : ackAt.toInstant(),
                rows.getString("issued_by"),
                rows.getTimestamp("created_at").toInstant(),
                rows.getTimestamp("expires_at").toInstant());
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final TypeReference<Map<String, Double>> PARAM_MAP =
            new TypeReference<>() {};

    private static String toJson(Map<String, Double> params) throws SQLException {
        try {
            return JSON.writeValueAsString(params);
        } catch (JsonProcessingException e) {
            throw new SQLException("cannot encode command parameters", e);
        }
    }

    private static Map<String, Double> fromJson(String json) throws SQLException {
        try {
            return JSON.readValue(json, PARAM_MAP);
        } catch (JsonProcessingException e) {
            throw new SQLException("cannot decode command parameters: " + json, e);
        }
    }
}
