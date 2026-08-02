package io.github.mustaffadnc.suru.protocol.mavlink;

/**
 * Extracts named numeric measurements from the MAVLink messages the platform stores.
 *
 * <p><b>The set is curated, not exhaustive, and that is the design.</b> MAVLink defines several
 * hundred messages; a platform that decoded all of them into a database would be storing mostly
 * things nobody queries, at the cost of a schema that has to track every dialect revision. What is
 * decoded here is what operators actually ask questions about — where the vehicle was, how it was
 * oriented, how it was powered, how good the fix was. Everything else stays in Kafka and in cold
 * storage, where it is cheap and still recoverable.
 *
 * <p><b>Field offsets are wire offsets, not declaration order.</b> MAVLink sorts a message's fields
 * by descending type size for transmission, so the order in the XML definition is frequently not
 * the order on the wire. Reading a message in declaration order produces plausible-looking numbers
 * that are silently wrong, which is why every offset below is stated explicitly rather than
 * computed from a field list.
 *
 * <p>Scaling is applied here too: MAVLink transmits positions as integer degrees × 10⁷ and
 * millimetres, and storing the raw integers would push the same conversion into every consumer and
 * every dashboard query.
 */
public final class MavlinkMetrics {

    /** HEARTBEAT. */
    public static final int MSG_HEARTBEAT = 0;

    /** SYS_STATUS — battery and link health. */
    public static final int MSG_SYS_STATUS = 1;

    /** GPS_RAW_INT — raw fix quality. */
    public static final int MSG_GPS_RAW_INT = 24;

    /** ATTITUDE — orientation. */
    public static final int MSG_ATTITUDE = 30;

    /** GLOBAL_POSITION_INT — fused position. */
    public static final int MSG_GLOBAL_POSITION_INT = 33;

    /** VFR_HUD — the pilot-facing summary. */
    public static final int MSG_VFR_HUD = 74;

    /** Receives one measurement. */
    @FunctionalInterface
    public interface MetricSink {
        /**
         * Called once per extracted measurement.
         *
         * @param metric metric name, stable across releases
         * @param value the value in its documented unit
         */
        void accept(String metric, double value);
    }

    private MavlinkMetrics() {
        throw new AssertionError("utility class");
    }

    /**
     * Whether this message contributes measurements.
     *
     * @param messageId MAVLink message id
     * @return {@code true} if {@link #extract} will emit anything
     */
    public static boolean isDecoded(int messageId) {
        return switch (messageId) {
            case MSG_SYS_STATUS, MSG_GPS_RAW_INT, MSG_ATTITUDE, MSG_GLOBAL_POSITION_INT, MSG_VFR_HUD ->
                    true;
            default -> false;
        };
    }

    /**
     * Emits the measurements carried by a frame.
     *
     * @param frame a decoded frame; its payload is read, not retained
     * @param sink receives each measurement
     */
    public static void extract(MavlinkFrame frame, MetricSink sink) {
        extract(frame.messageId(), frame, sink);
    }

    /**
     * Emits the measurements carried by a payload.
     *
     * <p>The overload the storage layer uses: by the time a record is read back off Kafka the
     * frame is long gone, and only the message id and the payload bytes remain.
     *
     * @param messageId MAVLink message id
     * @param payload the message payload
     * @param sink receives each measurement
     */
    public static void extract(int messageId, MavlinkPayload payload, MetricSink sink) {
        switch (messageId) {
            case MSG_GLOBAL_POSITION_INT -> globalPosition(payload, sink);
            case MSG_ATTITUDE -> attitude(payload, sink);
            case MSG_VFR_HUD -> vfrHud(payload, sink);
            case MSG_SYS_STATUS -> sysStatus(payload, sink);
            case MSG_GPS_RAW_INT -> gpsRaw(payload, sink);
            default -> {
                // Not a message this platform stores; the frame still reaches Kafka.
            }
        }
    }

    /** GLOBAL_POSITION_INT: uint32 time, int32 lat/lon/alt/relative_alt, int16 vx/vy/vz, uint16 hdg. */
    private static void globalPosition(MavlinkPayload f, MetricSink sink) {
        sink.accept("position.latitude_deg", f.i32(4) / 1e7);
        sink.accept("position.longitude_deg", f.i32(8) / 1e7);
        sink.accept("position.altitude_msl_m", f.i32(12) / 1000.0);
        sink.accept("position.altitude_rel_m", f.i32(16) / 1000.0);
        sink.accept("velocity.north_ms", f.i16(20) / 100.0);
        sink.accept("velocity.east_ms", f.i16(22) / 100.0);
        sink.accept("velocity.down_ms", f.i16(24) / 100.0);
        int heading = f.u16(26);
        if (heading != 65535) { // 65535 is MAVLink's "unknown"
            sink.accept("attitude.heading_deg", heading / 100.0);
        }
    }

    /** ATTITUDE: uint32 time, then six floats. */
    private static void attitude(MavlinkPayload f, MetricSink sink) {
        sink.accept("attitude.roll_rad", f.f32(4));
        sink.accept("attitude.pitch_rad", f.f32(8));
        sink.accept("attitude.yaw_rad", f.f32(12));
        sink.accept("attitude.rollspeed_rads", f.f32(16));
        sink.accept("attitude.pitchspeed_rads", f.f32(20));
        sink.accept("attitude.yawspeed_rads", f.f32(24));
    }

    /**
     * VFR_HUD: four floats first, then int16 heading and uint16 throttle.
     *
     * <p>A textbook case of wire order diverging from declaration order — {@code heading} and
     * {@code throttle} are declared between {@code groundspeed} and {@code alt} but transmitted
     * after both floats.
     */
    private static void vfrHud(MavlinkPayload f, MetricSink sink) {
        sink.accept("speed.airspeed_ms", f.f32(0));
        sink.accept("speed.groundspeed_ms", f.f32(4));
        sink.accept("position.altitude_hud_m", f.f32(8));
        sink.accept("speed.climb_ms", f.f32(12));
        sink.accept("attitude.heading_hud_deg", f.i16(16));
        sink.accept("power.throttle_pct", f.u16(18));
    }

    /** SYS_STATUS: three uint32 sensor masks, then load, voltage, current, comm counters. */
    private static void sysStatus(MavlinkPayload f, MetricSink sink) {
        sink.accept("power.load_pct", f.u16(12) / 10.0);

        int millivolts = f.u16(14);
        if (millivolts != 65535) { // unknown
            sink.accept("power.battery_v", millivolts / 1000.0);
        }
        int centiamps = f.i16(16);
        if (centiamps != -1) { // unknown
            sink.accept("power.current_a", centiamps / 100.0);
        }
        int remaining = f.i8(30);
        if (remaining != -1) { // unknown
            sink.accept("power.battery_remaining_pct", remaining);
        }
        sink.accept("link.drop_rate_pct", f.u16(18) / 100.0);
    }

    /** GPS_RAW_INT: uint64 time_usec, int32 lat/lon/alt, uint16 eph/epv/vel/cog, uint8 fix/sats. */
    private static void gpsRaw(MavlinkPayload f, MetricSink sink) {
        sink.accept("gps.fix_type", f.u8(28));
        sink.accept("gps.satellites", f.u8(29));

        int eph = f.u16(20);
        if (eph != 65535) { // unknown
            sink.accept("gps.hdop", eph / 100.0);
        }
    }
}
