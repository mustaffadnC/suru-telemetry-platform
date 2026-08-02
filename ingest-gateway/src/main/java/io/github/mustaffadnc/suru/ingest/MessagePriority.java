package io.github.mustaffadnc.suru.ingest;

/**
 * How much it costs the platform to lose a message.
 *
 * <p>Load shedding is only defensible if the thing being shed is chosen deliberately. Dropping
 * uniformly under pressure is the same as having no policy: it degrades exactly the signals the
 * platform makes decisions from, at exactly the moment it is under stress.
 *
 * <p>The classification below reflects what each message is actually used for downstream, not its
 * rate on the wire.
 */
public enum MessagePriority {

    /**
     * Never shed. Losing these does not degrade the picture — it corrupts it.
     *
     * <p>The load-bearing case is {@code HEARTBEAT}: absence of a heartbeat is precisely how the
     * platform concludes a vehicle is gone. Shedding heartbeats under load would manufacture
     * "telemetry lost" alarms for vehicles that are flying perfectly well, and it would do so
     * during the incident that caused the load — when operators can least afford to be lied to.
     */
    CRITICAL,

    /** The live picture: where the vehicle is and how it is oriented. Shed only in extremis. */
    HIGH,

    /** Useful telemetry with no immediate operational decision hanging on any single sample. */
    NORMAL,

    /**
     * High-rate diagnostics and estimator internals. Valuable in aggregate, individually
     * disposable — the first thing to go, and the reason a shedding policy is worth having at all.
     */
    BULK;

    /**
     * Classifies a MAVLink message.
     *
     * <p>Unknown ids are {@link #NORMAL} rather than {@link #BULK}: a message this build does not
     * recognise may matter to a newer dialect, and quietly making unknown things the most
     * disposable would hide the mistake.
     *
     * @param messageId MAVLink message id
     * @return the priority band
     */
    public static MessagePriority ofMavlink(int messageId) {
        return switch (messageId) {
            // Liveness, health, and the command path's correctness.
            case 0, // HEARTBEAT
                    1, // SYS_STATUS
                    77, // COMMAND_ACK
                    253 -> // STATUSTEXT
                    CRITICAL;

            // The live picture an operator is watching.
            case 24, // GPS_RAW_INT
                    30, // ATTITUDE
                    32, // LOCAL_POSITION_NED
                    33, // GLOBAL_POSITION_INT
                    74, // VFR_HUD
                    147 -> // BATTERY_STATUS
                    HIGH;

            // High-rate diagnostics and estimator internals.
            case 26, // SCALED_IMU
                    27, // RAW_IMU
                    35, // RC_CHANNELS_RAW
                    36, // SERVO_OUTPUT_RAW
                    65, // RC_CHANNELS
                    125, // POWER_STATUS
                    152, // MEMINFO
                    163, // AHRS
                    164, // SIMSTATE
                    165, // HWSTATUS
                    178, // AHRS2
                    241 -> // VIBRATION
                    BULK;

            default -> NORMAL;
        };
    }

    /**
     * Classifies a ÇARGE capsule log record.
     *
     * <p>The shape of the judgement is different here, because the traffic is. A capsule produces
     * a handful of irreplaceable records and a flood of routine ones:
     *
     * <ul>
     *   <li>{@code EVENT} is a mission state transition — release, descent, landing. There is
     *       exactly one of each per flight and losing it means losing when the capsule was
     *       released. Nothing else reconstructs that.
     *   <li>{@code META} appears once per log and identifies the firmware and reset cause; without
     *       it the rest of the file cannot be attributed to a build.
     *   <li>{@code IMU} arrives at 100 Hz. Any individual sample is disposable; the shape of the
     *       descent survives losing a great many of them.
     * </ul>
     *
     * @param recordType HK record type byte
     * @return the priority band
     */
    public static MessagePriority ofHk(int recordType) {
        return switch (recordType) {
            case 1, // META — one per log, identifies the build
                    5 -> // EVENT — mission state transition, one of each per flight
                    CRITICAL;
            case 2, // ENV — carries battery and mission state
                    4 -> // GPS — position fix
                    HIGH;
            case 3 -> BULK; // IMU — 100 Hz
            default -> NORMAL;
        };
    }

    /**
     * Whether this band may be discarded at a given pressure level.
     *
     * <p><b>Every threshold here sits above the read-pause watermark, on purpose.</b> Pausing a TCP
     * read is lossless — the socket buffer fills, the window closes, the sender slows down, and not
     * one message disappears. Shedding always loses something. So the cheap remedy is tried first,
     * and shedding begins only in the region where pausing has already been applied and pressure
     * kept climbing regardless. In practice that means one of two things: the backlog was already
     * in flight before the pause took effect, or the traffic is UDP and no pause was ever available.
     *
     * <p>An earlier revision had this backwards, shedding bulk at 0.50 while reads paused at 0.80 —
     * which threw data away while a lossless option was still untried.
     *
     * @param pressure admission pressure, {@code 0.0} idle to {@code 1.0} saturated
     * @return {@code true} if a message in this band should be shed
     */
    public boolean shouldShedAt(double pressure) {
        return switch (this) {
            case CRITICAL -> false;
            case HIGH -> pressure >= 0.95;
            case NORMAL -> pressure >= 0.85;
            case BULK -> pressure >= 0.75;
        };
    }
}
