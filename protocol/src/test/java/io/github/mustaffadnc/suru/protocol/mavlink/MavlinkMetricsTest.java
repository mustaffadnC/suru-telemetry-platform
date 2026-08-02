package io.github.mustaffadnc.suru.protocol.mavlink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.github.mustaffadnc.suru.protocol.TestResources;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MavlinkMetricsTest {

    private static Map<String, List<Double>> metrics;

    @BeforeAll
    static void decodeRecording() {
        byte[] stream = TestResources.bytes("/mavlink/sitl_stream.bin");
        MavlinkDecoder decoder = new MavlinkDecoder(MavlinkDialect.arduPilotMega());
        Map<String, List<Double>> collected = new HashMap<>();

        MavlinkDecoder.FrameHandler handler =
                frame ->
                        MavlinkMetrics.extract(
                                frame,
                                (metric, value) ->
                                        collected
                                                .computeIfAbsent(metric, k -> new ArrayList<>())
                                                .add(value));
        decoder.feed(stream, handler);
        decoder.endOfStream(handler);
        metrics = collected;
    }

    private static List<Double> values(String metric) {
        List<Double> found = metrics.get(metric);
        assertThat(found).as("metric %s was never emitted", metric).isNotNull();
        return found;
    }

    @Test
    @DisplayName("The recording decodes to where the aircraft actually was: Ankara")
    void decodesRealPositions() {
        // The SITL home position used across this portfolio is 39.925533, 32.866287. Wrong
        // field offsets would still yield plausible doubles — they would simply put the
        // aircraft somewhere else on Earth. Checking against a known location is what turns
        // "it parsed" into "it parsed correctly".
        //
        // The capture spans the autopilot acquiring its fix, so both states appear: zeros
        // before lock and the home coordinates after. Asserting every sample was in Ankara
        // failed on the pre-lock zeros, which is real data rather than a decode fault.
        assertThat(values("position.latitude_deg"))
                .anySatisfy(lat -> assertThat(lat).isCloseTo(39.925533, within(1e-6)));
        assertThat(values("position.longitude_deg"))
                .anySatisfy(lon -> assertThat(lon).isCloseTo(32.866287, within(1e-6)));

        // Both sides of the transition are present: no fix, then RTK fixed with ten satellites.
        assertThat(values("gps.fix_type")).contains(0.0, 6.0);
        assertThat(values("gps.satellites")).contains(0.0, 10.0);
    }

    @Test
    @DisplayName("Unknown sentinels are omitted, real values are kept")
    void distinguishesSentinelsFromValues() {
        // GPS_RAW_INT's eph is 65535 before lock and 121 after. Scaling the sentinel would
        // report an HDOP of 655.35 — not obviously wrong, which is the worst kind of wrong —
        // so it is skipped, while the genuine 1.21 is kept.
        List<Double> hdop = values("gps.hdop");
        assertThat(hdop).isNotEmpty().allSatisfy(v -> assertThat(v).isLessThan(100.0));
        assertThat(hdop).contains(1.21);
    }

    @Test
    @DisplayName("A position frame with known coordinates decodes to those coordinates")
    void decodesPositionOffsetsAndScaling() {
        // With the recording unable to exercise position, the offsets and the 1e7 scaling are
        // pinned here instead, against the home coordinates the rest of this portfolio uses.
        double latitude = 39.925533;
        double longitude = 32.866287;

        byte[] payload = new byte[28];
        writeInt(payload, 0, 1234); // time_boot_ms
        writeInt(payload, 4, (int) Math.round(latitude * 1e7));
        writeInt(payload, 8, (int) Math.round(longitude * 1e7));
        writeInt(payload, 12, 850_000); // 850 m MSL, in millimetres
        writeInt(payload, 16, 12_500); // 12.5 m relative
        writeShort(payload, 20, 150); // vx, cm/s
        writeShort(payload, 22, -75); // vy
        writeShort(payload, 24, 20); // vz
        writeShort(payload, 26, 27_000); // heading, cdeg

        Map<String, Double> decoded =
                decodeOne(MavlinkMetrics.MSG_GLOBAL_POSITION_INT, payload);

        assertThat(decoded.get("position.latitude_deg")).isEqualTo(latitude);
        assertThat(decoded.get("position.longitude_deg")).isEqualTo(longitude);
        assertThat(decoded.get("position.altitude_msl_m")).isEqualTo(850.0);
        assertThat(decoded.get("position.altitude_rel_m")).isEqualTo(12.5);
        assertThat(decoded.get("velocity.north_ms")).isEqualTo(1.5);
        assertThat(decoded.get("velocity.east_ms")).isEqualTo(-0.75);
        assertThat(decoded.get("attitude.heading_deg")).isEqualTo(270.0);
    }

    @Test
    @DisplayName("An unknown heading is omitted rather than reported as 655.35 degrees")
    void omitsUnknownHeading() {
        byte[] payload = new byte[28];
        writeShort(payload, 26, (short) 65535); // MAVLink's "unknown"

        Map<String, Double> decoded =
                decodeOne(MavlinkMetrics.MSG_GLOBAL_POSITION_INT, payload);

        // Scaling a sentinel produces a number that is not obviously wrong, which is the worst
        // kind of wrong: 655.35 is a plausible-looking bearing that no aircraft ever had.
        assertThat(decoded).doesNotContainKey("attitude.heading_deg");
    }

    @Test
    @DisplayName("Attitude is in radians and physically possible")
    void decodesAttitude() {
        assertThat(values("attitude.roll_rad"))
                .isNotEmpty()
                .allSatisfy(roll -> assertThat(Math.abs(roll)).isLessThanOrEqualTo(Math.PI));
        assertThat(values("attitude.pitch_rad"))
                .isNotEmpty()
                .allSatisfy(pitch -> assertThat(Math.abs(pitch)).isLessThanOrEqualTo(Math.PI / 2 + 0.01));
        assertThat(values("attitude.yaw_rad"))
                .isNotEmpty()
                .allSatisfy(yaw -> assertThat(Math.abs(yaw)).isLessThanOrEqualTo(Math.PI + 0.01));
    }

    @Test
    @DisplayName("Battery voltage decodes to the simulated pack's actual 12.6 V")
    void decodesBatteryVoltage() {
        // ArduPilot SITL simulates a 3S pack at 12.6 V. The very first SYS_STATUS of the boot
        // sequence reports 0 mV because the monitor has not initialised — that zero is real
        // data, not a decode failure, and is reported rather than filtered. Inventing a
        // plausible value for it would be worse than showing what the vehicle said.
        List<Double> volts = values("power.battery_v");
        assertThat(volts).isNotEmpty();
        assertThat(volts).contains(12.6);
        assertThat(volts.stream().filter(v -> v > 0).toList())
                .isNotEmpty()
                .allSatisfy(v -> assertThat(v).isBetween(6.0, 30.0));
        assertThat(values("power.battery_remaining_pct")).contains(100.0);
    }

    @Test
    @DisplayName("VFR_HUD reads heading and throttle from their wire offsets, not declaration order")
    void honoursWireFieldOrder() {
        // VFR_HUD declares heading and throttle between groundspeed and alt, but MAVLink sorts
        // fields by descending type size for transmission, so both integers move after all four
        // floats. Decoding in declaration order yields numbers that look reasonable and are
        // wrong — this frame is built with known values so that mistake cannot pass.
        byte[] payload = new byte[20];
        writeFloat(payload, 0, 12.5f); // airspeed
        writeFloat(payload, 4, 11.0f); // groundspeed
        writeFloat(payload, 8, 123.75f); // alt
        writeFloat(payload, 12, -1.5f); // climb
        payload[16] = (byte) 90; // heading, int16 little-endian
        payload[17] = 0;
        payload[18] = (byte) 75; // throttle, uint16
        payload[19] = 0;

        byte[] frame =
                MavlinkTestFrames.v2(
                        MavlinkDialect.arduPilotMega(),
                        0,
                        1,
                        1,
                        MavlinkMetrics.MSG_VFR_HUD,
                        payload);

        Map<String, Double> decoded = new HashMap<>();
        MavlinkDecoder decoder = new MavlinkDecoder(MavlinkDialect.arduPilotMega());
        MavlinkDecoder.FrameHandler handler =
                f -> MavlinkMetrics.extract(f, decoded::put);
        decoder.feed(frame, handler);
        decoder.endOfStream(handler);

        assertThat(decoded.get("speed.airspeed_ms")).isEqualTo(12.5);
        assertThat(decoded.get("speed.groundspeed_ms")).isEqualTo(11.0);
        assertThat(decoded.get("position.altitude_hud_m")).isEqualTo(123.75);
        assertThat(decoded.get("speed.climb_ms")).isEqualTo(-1.5);
        assertThat(decoded.get("attitude.heading_hud_deg")).isEqualTo(90.0);
        assertThat(decoded.get("power.throttle_pct")).isEqualTo(75.0);
    }

    @Test
    @DisplayName("A payload truncated by the sender decodes, with the missing tail read as zero")
    void toleratesTruncatedPayload() {
        // A v2 sender may drop trailing zero bytes. An accessor that treated a short payload as
        // an error would reject every message whose tail happens to be zero — which for
        // ATTITUDE at rest is most of them.
        byte[] payload = new byte[8]; // time + roll only; the rest was zero and got dropped
        writeFloat(payload, 4, 0.25f);

        byte[] frame =
                MavlinkTestFrames.v2(
                        MavlinkDialect.arduPilotMega(),
                        0,
                        1,
                        1,
                        MavlinkMetrics.MSG_ATTITUDE,
                        payload);

        Map<String, Double> decoded = new HashMap<>();
        MavlinkDecoder decoder = new MavlinkDecoder(MavlinkDialect.arduPilotMega());
        MavlinkDecoder.FrameHandler handler = f -> MavlinkMetrics.extract(f, decoded::put);
        decoder.feed(frame, handler);
        decoder.endOfStream(handler);

        assertThat(decoded.get("attitude.roll_rad")).isEqualTo(0.25);
        assertThat(decoded.get("attitude.yaw_rad")).isZero();
        assertThat(decoded.get("attitude.yawspeed_rads")).isZero();
    }

    @Test
    @DisplayName("Only the curated message set produces metrics")
    void onlyCuratedMessagesDecode() {
        assertThat(MavlinkMetrics.isDecoded(MavlinkMetrics.MSG_GLOBAL_POSITION_INT)).isTrue();
        assertThat(MavlinkMetrics.isDecoded(MavlinkMetrics.MSG_ATTITUDE)).isTrue();
        // SIMSTATE: simulator ground truth, deliberately not stored.
        assertThat(MavlinkMetrics.isDecoded(164)).isFalse();
        assertThat(MavlinkMetrics.isDecoded(MavlinkMetrics.MSG_HEARTBEAT)).isFalse();
    }

    /** Feeds a single synthetic frame through the decoder and collects its metrics. */
    private static Map<String, Double> decodeOne(int messageId, byte[] payload) {
        byte[] frame =
                MavlinkTestFrames.v2(
                        MavlinkDialect.arduPilotMega(), 0, 1, 1, messageId, payload);
        Map<String, Double> decoded = new HashMap<>();
        MavlinkDecoder decoder = new MavlinkDecoder(MavlinkDialect.arduPilotMega());
        MavlinkDecoder.FrameHandler handler = f -> MavlinkMetrics.extract(f, decoded::put);
        decoder.feed(frame, handler);
        decoder.endOfStream(handler);
        return decoded;
    }

    private static void writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) value;
        target[offset + 1] = (byte) (value >>> 8);
        target[offset + 2] = (byte) (value >>> 16);
        target[offset + 3] = (byte) (value >>> 24);
    }

    private static void writeShort(byte[] target, int offset, int value) {
        target[offset] = (byte) value;
        target[offset + 1] = (byte) (value >>> 8);
    }

    private static void writeFloat(byte[] target, int offset, float value) {
        int bits = Float.floatToIntBits(value);
        target[offset] = (byte) bits;
        target[offset + 1] = (byte) (bits >>> 8);
        target[offset + 2] = (byte) (bits >>> 16);
        target[offset + 3] = (byte) (bits >>> 24);
    }
}
