package io.github.mustaffadnc.suru.rules;

import java.util.Objects;
import java.util.OptionalDouble;

/**
 * Fires when a device leaves a circle centred on a fixed point.
 *
 * <p>Hysteresis is a second, smaller radius: the alert fires at {@code fireRadiusM} and does not
 * stop holding until the device is back inside {@code clearRadiusM}. A vehicle tracking the fence
 * line — which is exactly what one does when it has been told to hold station near the boundary —
 * would otherwise cross it repeatedly and generate an alert per GPS fix.
 *
 * <p><b>Both coordinates must be present.</b> Latitude and longitude arrive as two separate
 * measurements, and a rule evaluated when only one has landed would be comparing a fresh latitude
 * against a stale longitude — a position the vehicle was never at, somewhere on the line between
 * where it was and where it is. Missing either one means the condition does not hold; the device is
 * not assumed to be inside the fence, it is simply not assessed, and {@link Staleness} is what
 * notices a device that has stopped reporting position at all.
 *
 * @param latitudeMetric metric carrying latitude in degrees
 * @param longitudeMetric metric carrying longitude in degrees
 * @param centreLatitudeDeg centre of the fence
 * @param centreLongitudeDeg centre of the fence
 * @param fireRadiusM distance at which an inactive alert starts holding
 * @param clearRadiusM distance the device must come back inside for an active alert to stop
 */
public record Geofence(
        String latitudeMetric,
        String longitudeMetric,
        double centreLatitudeDeg,
        double centreLongitudeDeg,
        double fireRadiusM,
        double clearRadiusM)
        implements Condition {

    /** IUGG mean Earth radius, metres. */
    private static final double EARTH_RADIUS_M = 6_371_008.8;

    /** Rejects a clearing radius outside the firing one, which could never stop holding. */
    public Geofence {
        Objects.requireNonNull(latitudeMetric, "latitudeMetric");
        Objects.requireNonNull(longitudeMetric, "longitudeMetric");
        if (!(fireRadiusM > 0) || !Double.isFinite(fireRadiusM)) {
            throw new IllegalArgumentException("fireRadiusM must be positive and finite");
        }
        if (!(clearRadiusM > 0) || !Double.isFinite(clearRadiusM)) {
            throw new IllegalArgumentException("clearRadiusM must be positive and finite");
        }
        if (clearRadiusM > fireRadiusM) {
            throw new IllegalArgumentException(
                    "clearRadiusM %s exceeds fireRadiusM %s — the alert would clear outside the fence it fired on"
                            .formatted(clearRadiusM, fireRadiusM));
        }
    }

    /**
     * A fence over the standard position metrics, clearing at 90 % of the firing radius.
     *
     * @param centreLatitudeDeg centre of the fence
     * @param centreLongitudeDeg centre of the fence
     * @param radiusM the fence radius
     * @return the condition
     */
    public static Geofence around(
            double centreLatitudeDeg, double centreLongitudeDeg, double radiusM) {
        return new Geofence(
                "position.latitude_deg",
                "position.longitude_deg",
                centreLatitudeDeg,
                centreLongitudeDeg,
                radiusM,
                radiusM * 0.9);
    }

    @Override
    public boolean holds(Observation observation, boolean active) {
        OptionalDouble distance = distanceM(observation);
        if (distance.isEmpty()) {
            return false;
        }
        return distance.getAsDouble() > (active ? clearRadiusM : fireRadiusM);
    }

    @Override
    public String describe(Observation observation) {
        OptionalDouble distance = distanceM(observation);
        if (distance.isEmpty()) {
            return "position unavailable (fence radius %.0f m)".formatted(fireRadiusM);
        }
        return "%.0f m from fence centre (radius %.0f m)"
                .formatted(distance.getAsDouble(), fireRadiusM);
    }

    /**
     * Great-circle distance from the fence centre.
     *
     * @param observation the device's state
     * @return the distance in metres, or empty when either coordinate is missing
     */
    public OptionalDouble distanceM(Observation observation) {
        OptionalDouble lat = observation.value(latitudeMetric);
        OptionalDouble lon = observation.value(longitudeMetric);
        if (lat.isEmpty() || lon.isEmpty()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(
                haversineM(
                        centreLatitudeDeg, centreLongitudeDeg, lat.getAsDouble(), lon.getAsDouble()));
    }

    /**
     * Great-circle distance between two points.
     *
     * <p>Haversine rather than the equirectangular approximation: the cheap version is accurate
     * only for short distances at low latitudes, and a fence is exactly the place where being
     * wrong by a few percent decides whether an alert fires.
     *
     * @param lat1Deg first latitude
     * @param lon1Deg first longitude
     * @param lat2Deg second latitude
     * @param lon2Deg second longitude
     * @return distance in metres
     */
    public static double haversineM(double lat1Deg, double lon1Deg, double lat2Deg, double lon2Deg) {
        double lat1 = Math.toRadians(lat1Deg);
        double lat2 = Math.toRadians(lat2Deg);
        double dLat = lat2 - lat1;
        double dLon = Math.toRadians(lon2Deg - lon1Deg);

        double sinLat = Math.sin(dLat / 2);
        double sinLon = Math.sin(dLon / 2);
        double a = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;
        return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }
}
