package io.github.mustaffadnc.suru.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The command, outbox and audit-log schema, against a real PostgreSQL.
 *
 * <p>Checked functionally rather than by trusting that the migration ran. A constraint can be
 * missing while every statement in the file succeeded — a {@code UNIQUE} on the wrong columns, a
 * trigger attached to the wrong event — and the failure only shows up the day it was needed.
 */
class CommandSchemaIT {

    private static TimescaleTestDatabase db;

    @BeforeAll
    static void startDatabase() {
        db = TimescaleTestDatabase.startAndMigrate();
        execute(
                """
                INSERT INTO tenant (tenant_id, display_name) VALUES ('acme', 'Acme')
                ON CONFLICT DO NOTHING
                """);
    }

    @AfterAll
    static void stopDatabase() {
        if (db != null) {
            db.close();
        }
    }

    private static void execute(String sql) {
        try (Connection connection = db.dataSource().getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("failed: " + sql, e);
        }
    }

    /**
     * Inserts an ARM command.
     *
     * <p>Each test supplies its own device, because V6 allows only one unanswered command per
     * {@code (tenant, device, MAV_CMD id)} — a second ARM to the same vehicle would trip that
     * constraint rather than the one the test is about.
     */
    private static void insertCommand(String id, String idempotencyKey, String device) {
        execute(
                """
                INSERT INTO command (id, tenant_id, device_id, idempotency_key, command_type,
                                     mav_command_id, state, issued_by, expires_at)
                VALUES ('%s', 'acme', '%s', '%s', 'ARM', 400, 'PENDING', 'operator@acme',
                        now() + INTERVAL '30 seconds')
                """
                        .formatted(id, device, idempotencyKey));
    }

    @Test
    @DisplayName("the same idempotency key cannot issue a second command")
    void idempotencyKeyIsUniquePerTenant() throws SQLException {
        insertCommand("11111111-1111-1111-1111-111111111111", "retry-me", "link/sysA");

        assertThatThrownBy(
                        () ->
                                insertCommand(
                                        "22222222-2222-2222-2222-222222222222",
                                        "retry-me",
                                        "link/sysB"))
                .as("for ARM or TAKEOFF this is the difference between a retry and a second launch")
                .hasRootCauseInstanceOf(org.postgresql.util.PSQLException.class)
                .hasStackTraceContaining("command_tenant_id_idempotency_key_key");

        assertThat(db.queryOne("SELECT count(*) FROM command WHERE idempotency_key = 'retry-me'"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("two tenants may use the same idempotency key")
    void idempotencyIsScopedToTheTenant() throws SQLException {
        execute(
                """
                INSERT INTO tenant (tenant_id, display_name) VALUES ('other', 'Other')
                ON CONFLICT DO NOTHING
                """);
        insertCommand("33333333-3333-3333-3333-333333333333", "shared-key", "link/sysC");
        execute(
                """
                INSERT INTO command (id, tenant_id, device_id, idempotency_key, command_type,
                                     mav_command_id, state, issued_by, expires_at)
                VALUES ('44444444-4444-4444-4444-444444444444', 'other', 'link/sys1',
                        'shared-key', 'ARM', 400, 'PENDING', 'operator@other',
                        now() + INTERVAL '30 seconds')
                """);

        assertThat(db.queryOne("SELECT count(*) FROM command WHERE idempotency_key = 'shared-key'"))
                .as("a global key would let one tenant observe another's traffic")
                .isEqualTo("2");
    }

    @Test
    @DisplayName("an unknown command state is rejected")
    void stateIsConstrained() {
        assertThatThrownBy(
                        () ->
                                execute(
                                        """
                                        INSERT INTO command (id, tenant_id, device_id, idempotency_key,
                                                             command_type, mav_command_id, state,
                                                             issued_by, expires_at)
                                        VALUES ('55555555-5555-5555-5555-555555555555', 'acme',
                                                'link/sysF', 'bad-state', 'ARM', 400,
                                                'PROBABLY_FINE', 'operator@acme', now())
                                        """))
                .hasStackTraceContaining("command_state_check");
    }

    @Test
    @DisplayName("the outbox row and its command are written or lost together")
    void outboxCascadesWithItsCommand() throws SQLException {
        insertCommand("66666666-6666-6666-6666-666666666666", "outbox-test", "link/sysD");
        execute(
                """
                INSERT INTO command_outbox (command_id, topic, payload)
                VALUES ('66666666-6666-6666-6666-666666666666', 'commands.outbound', '{"a":1}')
                """);

        assertThat(db.queryOne("SELECT count(*) FROM command_outbox")).isEqualTo("1");

        execute("DELETE FROM command WHERE id = '66666666-6666-6666-6666-666666666666'");
        assertThat(db.queryOne("SELECT count(*) FROM command_outbox"))
                .as("an outbox row referring to no command would publish an unrecorded command")
                .isEqualTo("0");
    }

    @Test
    @DisplayName("the outbox id cannot be supplied by the application")
    void outboxIdIsGeneratedAlways() throws SQLException {
        insertCommand("77777777-7777-7777-7777-777777777777", "identity-test", "link/sysE");

        assertThatThrownBy(
                        () ->
                                execute(
                                        """
                                        INSERT INTO command_outbox (id, command_id, topic, payload)
                                        VALUES (1, '77777777-7777-7777-7777-777777777777',
                                                'commands.outbound', '{}')
                                        """))
                .as("the relay orders by this column, so an application-chosen value could jump the queue")
                .hasStackTraceContaining("cannot insert a non-DEFAULT value into column");
    }

    // --- the audit log -------------------------------------------------------------------

    private static void insertAudit(String actor, String outcome) {
        execute(
                """
                INSERT INTO audit_log (tenant_id, actor, action, subject, outcome)
                VALUES ('acme', '%s', 'command.issue', 'link/sys1', '%s')
                """
                        .formatted(actor, outcome));
    }

    @Test
    @DisplayName("the audit log accepts inserts")
    void auditAcceptsInserts() throws SQLException {
        insertAudit("operator@acme", "ALLOWED");

        assertThat(db.queryOne("SELECT count(*) FROM audit_log WHERE actor = 'operator@acme'"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("the audit log refuses UPDATE")
    void auditRefusesUpdate() {
        insertAudit("update-probe", "ALLOWED");

        assertThatThrownBy(
                        () ->
                                execute(
                                        "UPDATE audit_log SET outcome = 'ALLOWED'"
                                                + " WHERE actor = 'update-probe'"))
                .hasStackTraceContaining("append-only");
    }

    @Test
    @DisplayName("the audit log refuses DELETE")
    void auditRefusesDelete() {
        insertAudit("delete-probe", "DENIED");

        assertThatThrownBy(
                        () -> execute("DELETE FROM audit_log WHERE actor = 'delete-probe'"))
                .hasStackTraceContaining("append-only");
    }

    /**
     * The reason the triggers are {@code FOR EACH STATEMENT}.
     *
     * <p>A row-level trigger fires once per affected row and therefore never fires when nothing
     * matches. {@code DELETE FROM audit_log WHERE false} would then succeed, and an operator
     * probing whether deletion is possible would be told that it is. The statement-level trigger
     * refuses the attempt rather than the effect.
     */
    @Test
    @DisplayName("a DELETE matching no rows is still refused")
    void auditRefusesDeleteEvenWhenItWouldAffectNothing() {
        assertThatThrownBy(
                        () -> execute("DELETE FROM audit_log WHERE actor = 'nobody-by-that-name'"))
                .as("a row-level trigger would let this through and report deletion as permitted")
                .hasStackTraceContaining("append-only");
    }

    @Test
    @DisplayName("the audit log refuses TRUNCATE")
    void auditRefusesTruncate() {
        assertThatThrownBy(() -> execute("TRUNCATE audit_log"))
                .as("TRUNCATE bypasses row triggers entirely and is the obvious way round them")
                .hasStackTraceContaining("append-only");
    }

    @Test
    @DisplayName("an unknown audit outcome is rejected")
    void auditOutcomeIsConstrained() {
        assertThatThrownBy(() -> insertAudit("bad-outcome", "PROBABLY"))
                .hasStackTraceContaining("audit_log_outcome_check");
    }
}
