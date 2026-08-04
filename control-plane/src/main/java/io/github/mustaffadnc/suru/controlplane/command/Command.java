package io.github.mustaffadnc.suru.controlplane.command;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;

/**
 * One command issued to a vehicle.
 *
 * @param id this platform's identifier for the command, and the key an ACK is matched on
 * @param tenantId owning tenant
 * @param deviceId the vehicle
 * @param idempotencyKey the caller's key; reissuing with the same key returns this command
 * @param type what to do
 * @param params command parameters, meaning defined by {@code type}
 * @param state where the command stands
 * @param ackResult the vehicle's MAVLink result code, or {@code -1} if it has not answered
 * @param ackAt when the vehicle answered, or {@code null}
 * @param issuedBy who asked for it
 * @param createdAt when it was accepted
 * @param expiresAt when an unanswered command is declared timed out
 */
public record Command(
        UUID id,
        String tenantId,
        String deviceId,
        String idempotencyKey,
        CommandType type,
        Map<String, Double> params,
        CommandState state,
        int ackResult,
        Instant ackAt,
        String issuedBy,
        Instant createdAt,
        Instant expiresAt) {

    /** Sentinel for "the vehicle has not answered". */
    public static final int NO_ACK = -1;

    /** Copies the parameter map so a command cannot change after it is issued. */
    public Command {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(deviceId, "deviceId");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(issuedBy, "issuedBy");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(expiresAt, "expiresAt");
        params = Map.copyOf(params);
    }

    /**
     * The vehicle's result code, if it has answered.
     *
     * @return the code, or empty
     */
    public OptionalInt ack() {
        return ackResult == NO_ACK ? OptionalInt.empty() : OptionalInt.of(ackResult);
    }

    /**
     * Whether this command is still waiting on the vehicle.
     *
     * @return {@code true} in {@link CommandState#PENDING} and {@link CommandState#SENT}
     */
    public boolean awaitingAck() {
        return state.awaitingAck();
    }
}
