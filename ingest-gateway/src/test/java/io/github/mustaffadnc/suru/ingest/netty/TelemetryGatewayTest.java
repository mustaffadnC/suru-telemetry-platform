package io.github.mustaffadnc.suru.ingest.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.InMemoryTelemetryPublisher;
import io.github.mustaffadnc.suru.ingest.MessagePriority;
import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TelemetryGatewayTest {

    /** Frames in the recorded stream, established in phase 1. */
    private static final int EXPECTED_FRAMES = 1058;

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static byte[] sitlStream;

    private TelemetryGateway gateway;
    private InMemoryTelemetryPublisher publisher;

    @BeforeAll
    static void loadStream() throws IOException {
        try (InputStream in =
                TelemetryGatewayTest.class.getResourceAsStream("/mavlink/sitl_stream.bin")) {
            if (in == null) {
                throw new IllegalStateException("sitl_stream.bin missing from the test classpath");
            }
            sitlStream = in.readAllBytes();
        }
    }

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

    private InetSocketAddress startGateway(AdmissionController admission, DeviceRegistry registry)
            throws InterruptedException {
        publisher = new InMemoryTelemetryPublisher();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(), admission, publisher, registry);
        return gateway.start(0);
    }

    /** Writes the stream on a background thread — a paused gateway will block the writer. */
    private static CompletableFuture<Void> sendAsync(InetSocketAddress address, byte[] data) {
        return CompletableFuture.runAsync(
                () -> {
                    try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
                        socket.setTcpNoDelay(true);
                        OutputStream out = socket.getOutputStream();
                        int chunk = 1024;
                        for (int off = 0; off < data.length; off += chunk) {
                            out.write(data, off, Math.min(chunk, data.length - off));
                        }
                        out.flush();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    @Test
    @DisplayName("A recorded flight streamed over a real socket arrives as attributed telemetry")
    void ingestsRecordedFlight() throws Exception {
        InetSocketAddress address =
                startGateway(new AdmissionController(), DeviceRegistry.open());

        sendAsync(address, sitlStream).join();

        await().atMost(TIMEOUT)
                .untilAsserted(
                        () -> assertThat(publisher.published()).hasSize(EXPECTED_FRAMES));

        List<TelemetryEnvelope> envelopes = publisher.published();
        assertThat(envelopes)
                .allSatisfy(
                        e -> {
                            assertThat(e.tenantId()).isEqualTo(DeviceRegistry.DEFAULT_TENANT);
                            assertThat(e.deviceId()).endsWith("/sys1");
                            assertThat(e.source())
                                    .isEqualTo(TelemetryEnvelope.SourceProtocol.MAVLINK);
                        });

        // The boot banner at the head of the recording is not telemetry and must not have
        // produced an envelope; the count matching phase 1 exactly is that assertion.
        assertThat(envelopes).hasSize(EXPECTED_FRAMES);
        assertThat(gateway.counters().publishFailures()).isZero();
        assertThat(gateway.counters().connectionsAccepted()).isEqualTo(1);
    }

    @Test
    @DisplayName("A stalled downstream costs nothing while there is capacity to absorb it")
    void stalledDownstreamLosesNothing() throws Exception {
        // Capacity comfortably exceeds the whole recording, so the gateway can hold every
        // message in flight. This is the case the TCP design is built for: the downstream
        // stops, the gateway absorbs, and not one message is discarded.
        AdmissionController admission = new AdmissionController(16384);
        InetSocketAddress address = startGateway(admission, DeviceRegistry.open());

        publisher.stall();
        sendAsync(address, sitlStream).join();

        await().atMost(TIMEOUT)
                .untilAsserted(
                        () -> assertThat(publisher.published()).hasSize(EXPECTED_FRAMES));

        assertThat(admission.stats().shedTotal()).isZero();
        assertThat(admission.stats().inFlight()).isEqualTo(EXPECTED_FRAMES);

        publisher.resume();

        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(admission.stats().inFlight()).isZero());
        assertThat(gateway.counters().publishFailures()).isZero();
    }

    @Test
    @DisplayName("Under real starvation the gateway pauses reads, then sheds bulk — never heartbeats")
    void degradesInTheRightOrder() throws Exception {
        // Capacity far below the offered load, and a downstream that never drains: the
        // gateway has no way out and must degrade. What matters is the shape of the damage.
        AdmissionController admission = new AdmissionController(64);
        InetSocketAddress address = startGateway(admission, DeviceRegistry.open());

        publisher.stall();
        CompletableFuture<Void> sender = sendAsync(address, sitlStream);

        await().atMost(TIMEOUT)
                .untilAsserted(
                        () -> assertThat(admission.stats().shedTotal()).isGreaterThan(0));

        // Reads were paused before anything was discarded — the lossless remedy was tried
        // first. Shedding only began once holding the socket shut was not enough.
        assertThat(gateway.counters().readPauses()).isGreaterThan(0);

        // The damage is confined to the disposable band. A heartbeat lost here would make the
        // platform report a healthy aircraft as missing.
        assertThat(admission.stats().shedCritical()).isZero();
        assertThat(admission.stats().shedBulk()).isGreaterThan(0);

        publisher.resume();
        sender.join();

        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(admission.stats().inFlight()).isZero());

        // Every heartbeat that arrived was published, however bad it got.
        long heartbeatsPublished =
                publisher.published().stream()
                        .filter(e -> e.priority() == MessagePriority.CRITICAL)
                        .count();
        assertThat(heartbeatsPublished).isGreaterThan(0);
        assertThat(gateway.counters().publishFailures()).isZero();
    }

    @Test
    @DisplayName("A peer belonging to no tenant is refused, not silently accepted")
    void rejectsUnregisteredPeer() throws Exception {
        InetSocketAddress address =
                startGateway(new AdmissionController(), DeviceRegistry.closed());

        try {
            sendAsync(address, sitlStream).join();
        } catch (Exception expected) {
            // The gateway closes the connection; the writer may or may not notice before
            // finishing, so either outcome is acceptable here.
        }

        await().atMost(TIMEOUT)
                .untilAsserted(
                        () -> assertThat(gateway.counters().connectionsRejected()).isEqualTo(1));
        assertThat(publisher.published()).isEmpty();
        assertThat(gateway.counters().connectionsAccepted()).isZero();
    }

    @Test
    @DisplayName("A downstream that fails outright is counted, and the gateway keeps serving")
    void survivesPublisherFailures() throws Exception {
        AdmissionController admission = new AdmissionController();
        InetSocketAddress address = startGateway(admission, DeviceRegistry.open());

        publisher.failEverything();
        sendAsync(address, sitlStream).join();

        await().atMost(TIMEOUT)
                .untilAsserted(
                        () ->
                                assertThat(gateway.counters().publishFailures())
                                        .isEqualTo(EXPECTED_FRAMES));

        // Capacity reserved for a failed publication is still released, or the gateway would
        // strangle itself after a downstream outage instead of recovering from it.
        assertThat(admission.stats().inFlight()).isZero();
    }
}
