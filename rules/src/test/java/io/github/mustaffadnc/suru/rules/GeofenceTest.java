package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The fence, and the distance calculation under it. */
class GeofenceTest {

    private static final Instant NOW = Instant.parse("2026-08-04T12:00:00Z");
    private static final String LAT = "position.latitude_deg";
    private static final String LON = "position.longitude_deg";

    private static Observation at(double lat, double lon) {
        return new Observation("acme", "link/sys1", Map.of(LAT, lat, LON, lon), NOW, NOW);
    }

    /**
     * Two of these are analytic and need no reference at all: one degree of latitude is 2πR/360 by
     * definition, and an antipodal pair is πR. The rest come from an independent Python
     * implementation, the same discipline the protocol decoder is held to — a formula checked only
     * against itself is checked against nothing.
     */
    @Test
    @DisplayName("haversine agrees with the analytic cases and an independent implementation")
    void haversineIsCorrect() {
        assertThat(Geofence.haversineM(0, 0, 1, 0))
                .as("one degree of latitude = 2*pi*R/360")
                .isCloseTo(111_195.0802, within(0.001));
        assertThat(Geofence.haversineM(0, 0, 0, 180))
                .as("antipodal = pi*R")
                .isCloseTo(20_015_114.4420, within(0.001));

        assertThat(Geofence.haversineM(0, 0, 0, 1))
                .as("a degree of longitude at the equator equals a degree of latitude")
                .isCloseTo(111_195.0802, within(0.001));
        assertThat(Geofence.haversineM(60, 0, 60, 1))
                .as("and halves at 60 degrees north, where cos(60) = 1/2")
                .isCloseTo(55_597.0109, within(0.001));

        assertThat(Geofence.haversineM(39.9334, 32.8597, 41.0082, 28.9784))
                .as("Ankara to Istanbul")
                .isCloseTo(349_356.2218, within(0.001));
    }

    @Test
    @DisplayName("a device at the centre is inside")
    void centreIsInside() {
        Geofence fence = Geofence.around(39.8917, 32.7833, 1000);
        assertThat(fence.holds(at(39.8917, 32.7833), false)).isFalse();
    }

    @Test
    @DisplayName("crossing the fence radius fires")
    void leavingFires() {
        Geofence fence = Geofence.around(39.8917, 32.7833, 1000);

        // 500 m north: inside.
        assertThat(fence.holds(at(39.8917 + 500 / 111_194.93, 32.7833), false)).isFalse();
        // 1500 m north: outside.
        assertThat(fence.holds(at(39.8917 + 1500 / 111_194.93, 32.7833), false)).isTrue();
    }

    @Test
    @DisplayName("hysteresis keeps a fired fence held until the device is well back inside")
    void hysteresisBand() {
        Geofence fence = Geofence.around(39.8917, 32.7833, 1000);
        // 950 m out: inside the firing radius, outside the 900 m clearing radius.
        Observation justInside = at(39.8917 + 950 / 111_194.93, 32.7833);

        assertThat(fence.holds(justInside, false))
                .as("would not fire from rest")
                .isFalse();
        assertThat(fence.holds(justInside, true))
                .as("but does not release an alert that is already open")
                .isTrue();
    }

    @Test
    @DisplayName("a missing coordinate is not a violation")
    void missingCoordinateDoesNotFire() {
        Geofence fence = Geofence.around(39.8917, 32.7833, 1000);
        Observation latitudeOnly =
                new Observation("acme", "link/sys1", Map.of(LAT, 45.0), NOW, NOW);

        assertThat(fence.holds(latitudeOnly, false))
                .as("45N against a missing longitude is a position the vehicle was never at")
                .isFalse();
        assertThat(fence.distanceM(latitudeOnly)).isEmpty();
        assertThat(fence.describe(latitudeOnly)).contains("position unavailable");
    }

    @Test
    @DisplayName("a clearing radius outside the firing radius is rejected")
    void invertedRadiiRejected() {
        assertThatThrownBy(() -> new Geofence(LAT, LON, 39.0, 32.0, 500, 900))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("would clear outside the fence it fired on");
    }
}
