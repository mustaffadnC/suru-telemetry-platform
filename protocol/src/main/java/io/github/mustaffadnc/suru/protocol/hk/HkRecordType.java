package io.github.mustaffadnc.suru.protocol.hk;

/**
 * Record types written by the capsule's storage service.
 *
 * <p>Codes match {@code App/services/storage/storage.h} in the firmware.
 */
public enum HkRecordType {

    /** Boot metadata: firmware version, reset cause, log format version. */
    META(1),

    /** Environment sample: pressure, altitude, temperatures, humidity, battery. */
    ENV(2),

    /** Inertial sample: accelerometer, gyroscope, derived roll and pitch. */
    IMU(3),

    /** Position fix. */
    GPS(4),

    /** Mission state transition. */
    EVENT(5),

    /** A code this build does not know — forward compatibility, not an error. */
    UNKNOWN(-1);

    private final int code;

    HkRecordType(int code) {
        this.code = code;
    }

    /**
     * The on-the-wire code.
     *
     * @return the type byte, or {@code -1} for {@link #UNKNOWN}
     */
    public int code() {
        return code;
    }

    /**
     * Maps a wire code to its constant.
     *
     * @param code the type byte
     * @return the matching constant, or {@link #UNKNOWN}
     */
    public static HkRecordType fromCode(int code) {
        return switch (code) {
            case 1 -> META;
            case 2 -> ENV;
            case 3 -> IMU;
            case 4 -> GPS;
            case 5 -> EVENT;
            default -> UNKNOWN;
        };
    }
}
