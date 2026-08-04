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
     * MAV_CMD_COMPONENT_ARM_DISARM and are told apart only by {@link #param1()} — so a
     * COMMAND_ACK identifies the pair, not which of them was sent, and matching an ACK by command
     * id rather than by MAV_CMD id is what keeps a disarm from being credited to an arm.
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
