package io.github.mustaffadnc.suru.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The engine over a realistic rule set, including the three scenarios phase 4 has to demonstrate:
 * geofence breach, low battery, and telemetry loss.
 */
class RuleEngineTest {

    private static final Instant T0 = Instant.parse("2026-08-04T12:00:00Z");
    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";

    /** ODTÜ, roughly. */
    private static final double CENTRE_LAT = 39.8917;
    private static final double CENTRE_LON = 32.7833;

    private static final double METRES_PER_DEGREE_LAT = 111_194.93;

    private static RuleEngine fleetRules() {
        return new RuleEngine(
                List.of(
                        new Rule(
                                "geofence",
                                "Left the operating area",
                                TENANT,
                                Rule.ALL_DEVICES,
                                Geofence.around(CENTRE_LAT, CENTRE_LON, 1000),
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(10),
                                Severity.CRITICAL),
                        new Rule(
                                "battery",
                                "Battery low",
                                TENANT,
                                Rule.ALL_DEVICES,
                                new Threshold(
                                        "power.battery_remaining_pct",
                                        Threshold.Comparison.BELOW,
                                        20.0,
                                        25.0),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(30),
                                Severity.WARNING),
                        new Rule(
                                "silence",
                                "Telemetry lost",
                                TENANT,
                                Rule.ALL_DEVICES,
                                Staleness.after(Duration.ofSeconds(15)),
                                Duration.ZERO,
                                Duration.ZERO,
                                Severity.CRITICAL)));
    }

    /** A position the given number of metres due north of the fence centre. */
    private static Observation positionedAt(Observation base, double metresNorth, Instant at) {
        return base.with(
                        "position.latitude_deg",
                        CENTRE_LAT + metresNorth / METRES_PER_DEGREE_LAT,
                        at)
                .with("position.longitude_deg", CENTRE_LON, at);
    }

    @Test
    @DisplayName("scenario: a vehicle leaves the operating area")
    void geofenceBreach() {
        RuleEngine engine = fleetRules();
        AlertStateStore store = AlertStateStore.inMemory();
        Observation state = Observation.empty(TENANT, DEVICE, T0);

        // Inside, reporting every second: nothing fires.
        for (int t = 0; t < 10; t++) {
            state = positionedAt(state, 200, T0.plusSeconds(t));
            assertThat(engine.observe(state, store)).isEmpty();
        }

        // Crosses the fence. The debounce holds it for five seconds.
        state = positionedAt(state, 1500, T0.plusSeconds(10));
        assertThat(engine.observe(state, store)).isEmpty();

        state = positionedAt(state, 1600, T0.plusSeconds(14));
        assertThat(engine.observe(state, store)).isEmpty();

        state = positionedAt(state, 1700, T0.plusSeconds(15));
        List<Alert> alerts = engine.observe(state, store);

        assertThat(alerts).hasSize(1);
        Alert alert = alerts.getFirst();
        assertThat(alert.ruleId()).isEqualTo("geofence");
        assertThat(alert.severity()).isEqualTo(Severity.CRITICAL);
        assertThat(alert.kind()).isEqualTo(Alert.Kind.FIRED);
        assertThat(alert.conditionSince())
                .as("dated from the crossing, not from when the debounce expired")
                .isEqualTo(T0.plusSeconds(10));
        assertThat(alert.incidentalLatency()).isZero();
        assertThat(alert.detail()).contains("from fence centre");
    }

    @Test
    @DisplayName("scenario: the battery drains below the threshold and recovers")
    void batteryLowAndRecovered() {
        RuleEngine engine = fleetRules();
        AlertStateStore store = AlertStateStore.inMemory();
        Observation state =
                positionedAt(Observation.empty(TENANT, DEVICE, T0), 100, T0);

        state = state.with("power.battery_remaining_pct", 18.0, T0.plusSeconds(1));
        assertThat(engine.observe(state, store)).isEmpty();

        state = state.with("power.battery_remaining_pct", 17.0, T0.plusSeconds(31));
        List<Alert> fired = engine.observe(state, store);
        assertThat(fired).singleElement().extracting(Alert::ruleId).isEqualTo("battery");

        // A swap to a fresh pack. 30 % is above the 25 % clearing threshold.
        state = state.with("power.battery_remaining_pct", 30.0, T0.plusSeconds(40));
        assertThat(engine.observe(state, store)).as("recovery is debounced too").isEmpty();

        state = state.with("power.battery_remaining_pct", 30.0, T0.plusSeconds(70));
        List<Alert> resolved = engine.observe(state, store);
        assertThat(resolved).singleElement().extracting(Alert::kind).isEqualTo(Alert.Kind.RESOLVED);
    }

    @Test
    @DisplayName("scenario: a vehicle stops transmitting")
    void telemetryLoss() {
        RuleEngine engine = fleetRules();
        AlertStateStore store = AlertStateStore.inMemory();
        Observation state =
                positionedAt(Observation.empty(TENANT, DEVICE, T0), 100, T0)
                        .with("power.battery_remaining_pct", 90.0, T0);
        assertThat(engine.observe(state, store)).isEmpty();

        // The link drops. No records arrive, so nothing calls the engine — until a timer does.
        assertThat(engine.observe(state.asOf(T0.plusSeconds(10)), store))
                .as("ten seconds of silence is under the fifteen-second limit")
                .isEmpty();

        List<Alert> alerts = engine.observe(state.asOf(T0.plusSeconds(20)), store);
        assertThat(alerts).hasSize(1);
        assertThat(alerts.getFirst().ruleId()).isEqualTo("silence");
        assertThat(alerts.getFirst().detail()).contains("silent for");

        // The link comes back.
        state = state.with("power.battery_remaining_pct", 88.0, T0.plusSeconds(25));
        List<Alert> resolved = engine.observe(state, store);
        assertThat(resolved).singleElement().extracting(Alert::kind).isEqualTo(Alert.Kind.RESOLVED);
    }

    @Test
    @DisplayName("rules are independent: one device can hold two unrelated alerts")
    void rulesDoNotInterfere() {
        RuleEngine engine = fleetRules();
        AlertStateStore store = AlertStateStore.inMemory();

        Observation state =
                positionedAt(Observation.empty(TENANT, DEVICE, T0), 5000, T0)
                        .with("power.battery_remaining_pct", 5.0, T0);
        engine.observe(state, store);

        // Both debounces expire in the same step; both alerts come out, neither suppresses the
        // other.
        state = positionedAt(state, 5000, T0.plusSeconds(31))
                .with("power.battery_remaining_pct", 5.0, T0.plusSeconds(31));
        List<Alert> alerts = engine.observe(state, store);

        assertThat(alerts).extracting(Alert::ruleId).containsExactlyInAnyOrder("geofence", "battery");
    }

    @Test
    @DisplayName("a rule scoped to one device ignores the others")
    void deviceScoping() {
        RuleEngine engine =
                new RuleEngine(
                        List.of(
                                new Rule(
                                        "lead-only",
                                        "Lead vehicle battery",
                                        TENANT,
                                        "link/sys1",
                                        Threshold.at(
                                                "power.battery_remaining_pct",
                                                Threshold.Comparison.BELOW,
                                                50.0),
                                        Duration.ZERO,
                                        Duration.ZERO,
                                        Severity.INFO)));
        AlertStateStore store = AlertStateStore.inMemory();

        Observation other =
                Observation.empty(TENANT, "link/sys2", T0)
                        .with("power.battery_remaining_pct", 5.0, T0);
        assertThat(engine.observe(other, store)).isEmpty();

        Observation lead =
                Observation.empty(TENANT, "link/sys1", T0)
                        .with("power.battery_remaining_pct", 5.0, T0);
        assertThat(engine.observe(lead, store)).hasSize(1);
    }

    @Test
    @DisplayName("a rule never sees another tenant's devices")
    void tenantIsolation() {
        RuleEngine engine = fleetRules();
        AlertStateStore store = AlertStateStore.inMemory();

        Observation foreign =
                Observation.empty("other-tenant", DEVICE, T0)
                        .with("power.battery_remaining_pct", 1.0, T0)
                        .with("position.latitude_deg", 0.0, T0)
                        .with("position.longitude_deg", 0.0, T0);

        assertThat(engine.observe(foreign.asOf(T0.plusSeconds(600)), store))
                .as("every rule here would fire on this data, if it belonged to the tenant")
                .isEmpty();
    }

    @Test
    @DisplayName("each device carries its own state under a wildcard rule")
    void perDeviceState() {
        RuleEngine engine = fleetRules();
        AlertStateStore store = AlertStateStore.inMemory();

        Observation one =
                positionedAt(Observation.empty(TENANT, "link/sys1", T0), 5000, T0);
        Observation two =
                positionedAt(Observation.empty(TENANT, "link/sys2", T0), 100, T0);

        engine.observe(one, store);
        engine.observe(two, store);

        // sys1's geofence debounce expires; sys2 is well inside and must stay quiet.
        List<Alert> first =
                engine.observe(positionedAt(one, 5000, T0.plusSeconds(6)), store);
        List<Alert> second =
                engine.observe(positionedAt(two, 100, T0.plusSeconds(6)), store);

        assertThat(first).singleElement().extracting(Alert::deviceId).isEqualTo("link/sys1");
        assertThat(second).isEmpty();
    }
}
