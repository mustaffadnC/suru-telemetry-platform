package io.github.mustaffadnc.suru.protocol.mavlink;

/**
 * Building COMMAND_LONG and reading COMMAND_ACK.
 *
 * <h2>MAV_CMD ids are not message ids</h2>
 *
 * <p>They are separate namespaces that happen to share small integers, and confusing them produces
 * a frame that is structurally valid and completely wrong. {@code MAV_CMD_COMPONENT_ARM_DISARM} is
 * 400 and travels <em>inside</em> a COMMAND_LONG's {@code command} field; message id 400 is an
 * unrelated 254-byte message. Nothing in the wire format catches the mistake — the dialect will
 * happily supply a CRC_EXTRA for either.
 *
 * <p>So this class takes a MAV_CMD id and always emits message {@link #MSG_COMMAND_LONG}.
 */
public final class MavlinkCommands {

    /** COMMAND_LONG message id. */
    public static final int MSG_COMMAND_LONG = 76;

    /** COMMAND_ACK message id. */
    public static final int MSG_COMMAND_ACK = 77;

    /** COMMAND_LONG payload length, confirmed against the dialect. */
    public static final int COMMAND_LONG_LENGTH = 33;

    /** MAV_RESULT_ACCEPTED. */
    public static final int RESULT_ACCEPTED = 0;

    private MavlinkCommands() {}

    /**
     * Builds a COMMAND_LONG payload.
     *
     * <p>Field order is the wire order, which is <b>not</b> the declaration order: MAVLink sorts
     * fields by descending type size, so the seven floats come first and the {@code command} field
     * that names the whole message sits at offset 28, after them. Writing the struct in declaration
     * order produces a frame every receiver misreads.
     *
     * @param mavCommandId the MAV_CMD id, e.g. 400 for arm/disarm
     * @param targetSystem the vehicle's MAVLink system id
     * @param targetComponent the component, usually 1
     * @param confirmation 0 for a first attempt, incrementing for retries
     * @param params the seven command parameters
     * @return a 33-byte payload
     */
    public static byte[] commandLong(
            int mavCommandId,
            int targetSystem,
            int targetComponent,
            int confirmation,
            float... params) {

        if (params.length > 7) {
            throw new IllegalArgumentException(
                    "COMMAND_LONG carries seven parameters, not " + params.length);
        }
        byte[] payload = new byte[COMMAND_LONG_LENGTH];
        for (int i = 0; i < params.length; i++) {
            putFloat(payload, i * 4, params[i]);
        }
        putUnsigned16(payload, 28, mavCommandId);
        payload[30] = (byte) targetSystem;
        payload[31] = (byte) targetComponent;
        payload[32] = (byte) confirmation;
        return payload;
    }

    /**
     * The MAV_CMD id a COMMAND_ACK is answering.
     *
     * <p>At offset 0, so it survives any truncation: COMMAND_ACK is 3 bytes at minimum and 10 with
     * its extension fields, and a v2 sender may send the short form.
     *
     * @param payload the ACK payload
     * @return the MAV_CMD id
     */
    public static int ackCommand(MavlinkPayload payload) {
        return payload.u16(0);
    }

    /**
     * The result code a COMMAND_ACK carries.
     *
     * @param payload the ACK payload
     * @return the MAV_RESULT value, {@link #RESULT_ACCEPTED} meaning accepted
     */
    public static int ackResult(MavlinkPayload payload) {
        return payload.u8(2);
    }

    private static void putFloat(byte[] target, int offset, float value) {
        int bits = Float.floatToRawIntBits(value);
        target[offset] = (byte) (bits & 0xFF);
        target[offset + 1] = (byte) ((bits >>> 8) & 0xFF);
        target[offset + 2] = (byte) ((bits >>> 16) & 0xFF);
        target[offset + 3] = (byte) ((bits >>> 24) & 0xFF);
    }

    private static void putUnsigned16(byte[] target, int offset, int value) {
        target[offset] = (byte) (value & 0xFF);
        target[offset + 1] = (byte) ((value >>> 8) & 0xFF);
    }
}
