package io.github.mustaffadnc.suru.ingest.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.InMemoryTelemetryPublisher;
import io.github.mustaffadnc.suru.ingest.LoadHarness;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Many vehicles at once.
 *
 * <p>Sized to stay reliable on a shared CI runner. The throughput figures quoted in
 * {@code docs/benchmarks.md} come from a larger run on the reference machine; what is asserted here
 * is correctness under concurrency, not a performance number, because a number asserted on
 * unpredictable hardware is a flaky test rather than a measurement.
 */
class GatewayLoadTest {

    private static final int FRAMES_PER_STREAM = 1058;

    private TelemetryGateway gateway;
    private InMemoryTelemetryPublisher publisher;

    @AfterEach
    void tearDown() {
        if (gateway != null) {
            gateway.close();
            gateway = null;
        }
        if (publisher != null) {
            publisher.close();
            publisher = null;
        }
    }

    @Test
    @DisplayName("Eight simultaneous vehicles are ingested without loss and kept distinct")
    void handlesConcurrentVehicles() throws Exception {
        int connections = 8;
        int repeats = 2;
        int expected = connections * repeats * FRAMES_PER_STREAM;

        AdmissionController admission = new AdmissionController();
        publisher = new InMemoryTelemetryPublisher();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(),
                        admission,
                        publisher,
                        DeviceRegistry.open());
        InetSocketAddress address = gateway.start(0);

        byte[] stream = LoadHarness.recordedStream();
        LoadHarness.Result result =
                LoadHarness.runTcp(address, stream, connections, repeats, FRAMES_PER_STREAM);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(publisher.published()).hasSize(expected));

        // Every connection is its own link and therefore its own device, even though all eight
        // replays carry MAVLink system id 1. Without qualifying the device id by link, eight
        // aircraft would collapse into one and their telemetry would interleave into nonsense.
        Set<String> devices =
                publisher.published().stream()
                        .map(e -> e.deviceId())
                        .collect(Collectors.toSet());
        assertThat(devices).hasSize(connections);

        assertThat(admission.stats().shedTotal()).isZero();
        assertThat(gateway.counters().publishFailures()).isZero();
        assertThat(gateway.counters().connectionsAccepted()).isEqualTo(connections);

        System.out.println("[load] offered " + result);
    }

    @Test
    @EnabledIfSystemProperty(
            named = "suru.loadtest",
            matches = "true",
            disabledReason =
                    "measurement run, not a correctness check; enable with -Dsuru.loadtest=true")
    @DisplayName("Measurement: sustained ingest throughput")
    void measureSustainedThroughput() throws Exception {
        int connections = Integer.getInteger("suru.loadtest.connections", 32);
        int repeats = Integer.getInteger("suru.loadtest.repeats", 20);
        int expected = connections * repeats * FRAMES_PER_STREAM;

        AdmissionController admission = new AdmissionController(262144);
        publisher = new InMemoryTelemetryPublisher();
        // Retaining 677k envelopes would measure the allocator and the collector as much as the
        // gateway; the count is what this run is after.
        publisher.stopRecording();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(),
                        admission,
                        publisher,
                        DeviceRegistry.open());
        InetSocketAddress address = gateway.start(0);

        byte[] stream = LoadHarness.recordedStream();
        long start = System.nanoTime();
        LoadHarness.Result offered =
                LoadHarness.runTcp(address, stream, connections, repeats, FRAMES_PER_STREAM);

        await().atMost(Duration.ofMinutes(5))
                .untilAsserted(
                        () -> assertThat(publisher.publishedCount()).isEqualTo(expected));
        long ingestNanos = System.nanoTime() - start;

        double seconds = ingestNanos / 1_000_000_000.0;
        double megabytes = (double) stream.length * connections * repeats / (1024 * 1024);

        System.out.printf(
                "%n[measurement] %d connections x %d replays%n"
                        + "  offered      : %s%n"
                        + "  ingested     : %,d frames in %.2f s%n"
                        + "  throughput   : %.1f MB/s, %,.0f frames/s%n"
                        + "  admission    : %s%n"
                        + "  gateway      : %s%n%n",
                connections,
                repeats,
                offered,
                expected,
                seconds,
                megabytes / seconds,
                expected / seconds,
                admission.stats(),
                gateway.counters());

        assertThat(admission.stats().shedTotal()).isZero();
        assertThat(gateway.counters().publishFailures()).isZero();
    }

    @Test
    @DisplayName("A downstream outage mid-flight costs nothing once it clears")
    void recoversFromDownstreamOutage() throws Exception {
        int connections = 4;
        int repeats = 2;
        int expected = connections * repeats * FRAMES_PER_STREAM;

        AdmissionController admission = new AdmissionController(65536);
        publisher = new InMemoryTelemetryPublisher();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(),
                        admission,
                        publisher,
                        DeviceRegistry.open());
        InetSocketAddress address = gateway.start(0);

        // The broker goes away in the middle of the flight, then comes back. This is the
        // "docker stop kafka" scenario, run deterministically: with capacity to absorb the
        // outage the gateway holds everything and loses nothing.
        publisher.stall();
        byte[] stream = LoadHarness.recordedStream();
        LoadHarness.runTcp(address, stream, connections, repeats, FRAMES_PER_STREAM);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(publisher.published()).hasSize(expected));
        assertThat(admission.stats().shedTotal()).isZero();
        assertThat(admission.stats().inFlight()).isEqualTo(expected);

        publisher.resume();

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(admission.stats().inFlight()).isZero());
        assertThat(gateway.counters().publishFailures()).isZero();
    }
}
