package io.github.mustaffadnc.suru.protocol.hk;

import java.util.HexFormat;

/**
 * Payload layouts of the ÇARGE log records, and a canonical text rendering of them.
 *
 * <p>Offsets follow the packed little-endian structs the firmware writes; there is no padding
 * between fields. They are stated as constants rather than derived, because a wrong offset is
 * exactly the sort of bug that produces plausible-looking nonsense instead of a crash.
 *
 * <p><b>Why the canonical rendering prints floats as hex.</b> {@link #canonicalLine} exists so this
 * decoder can be diffed against the independent Python reader that ships with the firmware. If both
 * sides printed decimals, the comparison would be testing the two languages' float formatting —
 * Java rounds half-up, Python rounds half-even — rather than testing whether the same bytes were
 * read from the same offsets. Emitting the raw IEEE-754 bits removes formatting from the equation
 * and makes any difference a real one.
 */
public final class HkPayloads {

    /** Mission states, in the order the firmware's state machine declares them. */
    public static final String[] STATE_NAMES = {
        "BOOT", "SELFTEST", "ATTACHED", "ARMED", "RELEASE", "DESCENT", "LANDED", "RECOVERY"
    };

    /** Payload length of a {@link HkRecordType#META} record. */
    public static final int META_LENGTH = 8;

    /** Payload length of an {@link HkRecordType#ENV} record. */
    public static final int ENV_LENGTH = 41;

    /** Payload length of an {@link HkRecordType#IMU} record. */
    public static final int IMU_LENGTH = 36;

    /** Payload length of a {@link HkRecordType#GPS} record. */
    public static final int GPS_LENGTH = 35;

    /** Payload length of an {@link HkRecordType#EVENT} record. */
    public static final int EVENT_LENGTH = 8;

    private HkPayloads() {
        throw new AssertionError("utility class");
    }

    /**
     * Names a mission state.
     *
     * @param state the state index
     * @return the state name, or {@code ?<n>} when out of range
     */
    public static String stateName(int state) {
        return state >= 0 && state < STATE_NAMES.length ? STATE_NAMES[state] : "?" + state;
    }

    /** Boot metadata. */
    public record Meta(long timeMs, int firmwareVersion, int resetCause, int logVersion) {
        /**
         * Decodes a META payload.
         *
         * @param r the record
         * @return the decoded value
         */
        public static Meta of(HkRecord r) {
            return new Meta(r.u32(0), r.u16(4), r.u8(6), r.u8(7));
        }
    }

    /** Environment sample. */
    public record Env(
            long timeMs,
            int state,
            float pressurePa,
            float altitudeM,
            float baroTempC,
            float temp1C,
            float humidity1Pct,
            float temp2C,
            float humidity2Pct,
            float batteryV,
            float stateOfCharge) {
        /**
         * Decodes an ENV payload.
         *
         * @param r the record
         * @return the decoded value
         */
        public static Env of(HkRecord r) {
            return new Env(
                    r.u32(0),
                    r.u8(4),
                    r.f32(5),
                    r.f32(9),
                    r.f32(13),
                    r.f32(17),
                    r.f32(21),
                    r.f32(25),
                    r.f32(29),
                    r.f32(33),
                    r.f32(37));
        }
    }

    /** Inertial sample. */
    public record Imu(
            long timeMs,
            float accelX,
            float accelY,
            float accelZ,
            float gyroX,
            float gyroY,
            float gyroZ,
            float rollDeg,
            float pitchDeg) {
        /**
         * Decodes an IMU payload.
         *
         * @param r the record
         * @return the decoded value
         */
        public static Imu of(HkRecord r) {
            return new Imu(
                    r.u32(0),
                    r.f32(4),
                    r.f32(8),
                    r.f32(12),
                    r.f32(16),
                    r.f32(20),
                    r.f32(24),
                    r.f32(28),
                    r.f32(32));
        }
    }

    /** Position fix. */
    public record Gps(
            long timeMs,
            double latitude,
            double longitude,
            float altitudeM,
            float speedMs,
            float courseDeg,
            int satellites,
            int fixQuality,
            boolean valid) {
        /**
         * Decodes a GPS payload.
         *
         * @param r the record
         * @return the decoded value
         */
        public static Gps of(HkRecord r) {
            return new Gps(
                    r.u32(0),
                    r.f64(4),
                    r.f64(12),
                    r.f32(20),
                    r.f32(24),
                    r.f32(28),
                    r.u8(32),
                    r.u8(33),
                    r.u8(34) != 0);
        }
    }

    /** Mission state transition. */
    public record Event(long timeMs, int fromState, int toState, int argument) {
        /**
         * Decodes an EVENT payload.
         *
         * @param r the record
         * @return the decoded value
         */
        public static Event of(HkRecord r) {
            return new Event(r.u32(0), r.u8(4), r.u8(5), r.u16(6));
        }
    }

    /**
     * Renders a record in the canonical diff format, floats as raw IEEE-754 hex.
     *
     * @param r the record
     * @return one line, without a trailing newline
     */
    public static String canonicalLine(HkRecord r) {
        return switch (r.recordType()) {
            case META ->
                    "META,t=%d,fw=%d,reset=%d,logver=%d"
                            .formatted(r.u32(0), r.u16(4), r.u8(6), r.u8(7));
            case ENV ->
                    "ENV,t=%d,state=%d,p=%s,alt=%s,tbmp=%s,t1=%s,rh1=%s,t2=%s,rh2=%s,vbat=%s,soc=%s"
                            .formatted(
                                    r.u32(0),
                                    r.u8(4),
                                    f32(r, 5),
                                    f32(r, 9),
                                    f32(r, 13),
                                    f32(r, 17),
                                    f32(r, 21),
                                    f32(r, 25),
                                    f32(r, 29),
                                    f32(r, 33),
                                    f32(r, 37));
            case IMU ->
                    "IMU,t=%d,ax=%s,ay=%s,az=%s,gx=%s,gy=%s,gz=%s,roll=%s,pitch=%s"
                            .formatted(
                                    r.u32(0),
                                    f32(r, 4),
                                    f32(r, 8),
                                    f32(r, 12),
                                    f32(r, 16),
                                    f32(r, 20),
                                    f32(r, 24),
                                    f32(r, 28),
                                    f32(r, 32));
            case GPS ->
                    "GPS,t=%d,lat=%s,lon=%s,alt=%s,spd=%s,crs=%s,sats=%d,fix=%d,valid=%d"
                            .formatted(
                                    r.u32(0),
                                    f64(r, 4),
                                    f64(r, 12),
                                    f32(r, 20),
                                    f32(r, 24),
                                    f32(r, 28),
                                    r.u8(32),
                                    r.u8(33),
                                    r.u8(34));
            case EVENT ->
                    "EVENT,t=%d,from=%d,to=%d,arg=%d"
                            .formatted(r.u32(0), r.u8(4), r.u8(5), r.u16(6));
            case UNKNOWN ->
                    "UNKNOWN,type=%d,len=%d,payload=%s"
                            .formatted(
                                    r.type(),
                                    r.payloadLength(),
                                    HexFormat.of().formatHex(r.copyPayload()));
        };
    }

    private static String f32(HkRecord r, int index) {
        return "%08x".formatted((int) r.u32(index));
    }

    private static String f64(HkRecord r, int index) {
        return "%016x".formatted(Double.doubleToRawLongBits(r.f64(index)));
    }
}
