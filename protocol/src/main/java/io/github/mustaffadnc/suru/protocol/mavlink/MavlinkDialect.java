package io.github.mustaffadnc.suru.protocol.mavlink;

/**
 * Per-message metadata a MAVLink decoder needs but cannot derive from the wire format.
 *
 * <p>A MAVLink v2 checksum is not computed over the frame alone: a {@code CRC_EXTRA} byte, derived
 * from the message's field definitions, is fed in last. Without it a frame cannot be validated at
 * all — which is why a decoder is always bound to a dialect.
 *
 * <p>Messages outside the dialect are reported as {@link #UNKNOWN} rather than guessed at. That
 * distinction matters: the official C library folds unknown messages into {@code BAD_CRC}, so a
 * ground station running the plain {@code common} dialect against ArduPilot reports a stream full
 * of checksum errors and a skewed sequence-loss count. Here an unrecognised message is counted
 * separately and never inflates the corruption statistics.
 */
public interface MavlinkDialect {

    /** Returned by every lookup for a message id this dialect does not define. */
    int UNKNOWN = -1;

    /** The ArduPilotMega dialect, which is what ArduPilot and its SITL emit. */
    static MavlinkDialect arduPilotMega() {
        return ArduPilotMegaDialect.INSTANCE;
    }

    /**
     * The {@code CRC_EXTRA} byte for a message.
     *
     * @param msgId 24-bit MAVLink message id
     * @return the byte as {@code 0..255}, or {@link #UNKNOWN}
     */
    int crcExtra(int msgId);

    /**
     * Largest payload this message can carry, extension fields included.
     *
     * @param msgId 24-bit MAVLink message id
     * @return length in bytes, or {@link #UNKNOWN}
     */
    int maxPayloadLength(int msgId);

    /**
     * Smallest payload this message can carry, i.e. its length before any extension fields were
     * added. A v2 sender may truncate trailing zero bytes, so a valid frame's payload can be
     * shorter than this — the value is a diagnostic aid, not a validation bound.
     *
     * @param msgId 24-bit MAVLink message id
     * @return length in bytes, or {@link #UNKNOWN}
     */
    int minPayloadLength(int msgId);

    /**
     * How many messages this dialect defines.
     *
     * @return message count
     */
    int messageCount();

    /**
     * Dialect name, for diagnostics.
     *
     * @return e.g. {@code "ardupilotmega"}
     */
    String name();
}
