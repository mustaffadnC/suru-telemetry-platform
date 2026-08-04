package io.github.mustaffadnc.suru.controlplane.command;

/**
 * The commands this platform will issue.
 *
 * <p>A closed set rather than an arbitrary MAVLink command id. MAV_CMD defines hundreds of
 * commands, most of which have no business being reachable from a REST endpoint, and several of
 * which reconfigure a vehicle in ways no operator would choose deliberately from a dashboard.
 * Anything not listed here cannot be sent, which is a smaller and much more defensible surface than
 * validating an integer.
 */
public enum CommandType {
    /** MAV_CMD_COMPONENT_ARM_DISARM, param1 = 1. */
    ARM(400, 1.0f),
    /** MAV_CMD_COMPONENT_ARM_DISARM, param1 = 0. */
    DISARM(400, 0.0f),
    /** MAV_CMD_NAV_TAKEOFF; param7 carries the target altitude in metres. */
    TAKEOFF(22, 0.0f),
    /** MAV_CMD_NAV_LAND. */
    LAND(21, 0.0f),
    /** MAV_CMD_NAV_RETURN_TO_LAUNCH. */
    RETURN_TO_LAUNCH(20, 0.0f);

    private final int mavCommandId;
    private final float param1;

    CommandType(int mavCommandId, float param1) {
        this.mavCommandId = mavCommandId;
        this.param1 = param1;
    }

    /**
     * The MAV_CMD id this maps to.
     *
     * <p><b>{@link #ARM} and {@link #DISARM} share one.</b> Both are
     * MAV_CMD_COMPONENT_ARM_DISARM, told apart only by {@link #param1()}, so a COMMAND_ACK for 400
     * identifies the pair and not which of them was sent.
     *
     * <p>There is no way to resolve that from the answer. COMMAND_ACK carries {@code command},
     * {@code result}, {@code progress}, {@code result_param2} and the target ids — and no
     * correlation id. The vehicle never receives this platform's command id and cannot echo it
     * back, so an ACK can only ever be matched on {@code (device, MAV_CMD id)}.
     *
     * <p>So the ambiguity is prevented instead of resolved: at most one unanswered command per
     * {@code (tenant, device, MAV_CMD id)}, enforced by a partial unique index in migration
     * {@code V6}. Guessing would credit a disarm to an arm, and report a vehicle armed at the
     * moment it was disarmed.
     *
     * @return the command id
     */
    public int mavCommandId() {
        return mavCommandId;
    }

    /**
     * The fixed first parameter for this command.
     *
     * @return param1 as MAVLink expects it
     */
    public float param1() {
        return param1;
    }
}
