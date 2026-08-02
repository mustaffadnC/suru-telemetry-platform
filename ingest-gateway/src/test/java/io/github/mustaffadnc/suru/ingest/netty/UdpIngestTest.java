package io.github.mustaffadnc.suru.ingest.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.InMemoryTelemetryPublisher;
import io.github.mustaffadnc.suru.ingest.LoadHarness;
import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UdpIngestTest {

    private static final int FRAMES_IN_RECORDING = 1058;

    private static byte[] stream;
    private static List<byte[]> frames;

    private TelemetryGateway gateway;
    private InMemoryTelemetryPublisher publisher;

    @BeforeAll
    static void loadStream() {
        stream = LoadHarness.recordedStream();
        frames = LoadHarness.splitIntoFrames(stream);
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

    private InetSocketAddress startUdpGateway(AdmissionController admission, DeviceRegistry registry)
            throws InterruptedException {
        publisher = new InMemoryTelemetryPublisher();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(), admission, publisher, registry);
        return gateway.startUdp(0);
    }

    @Test
    @DisplayName("Frame-aligned datagrams decode and are attributed to their sender")
    void ingestsDatagrams() throws Exception {
        InetSocketAddress address =
                startUdpGateway(new AdmissionController(), DeviceRegistry.open());

        assertThat(frames).hasSize(FRAMES_IN_RECORDING);
        LoadHarness.sendUdp(address, frames, 50);

        // Deliberately not asserting an exact count. UDP loses datagrams — even on loopback,
        // once the receive buffer is momentarily full — and that loss is invisible to the
        // application. Demanding all 1058 here would produce a flaky test whose failures said
        // nothing about the code. That this assertion cannot be tightened is precisely the
        // property ADR-0003 describes.
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(
                        () ->
                                assertThat(publisher.published().size())
                                        .isGreaterThan((int) (FRAMES_IN_RECORDING * 0.80)));

        List<TelemetryEnvelope> envelopes = publisher.published();
        assertThat(envelopes)
                .allSatisfy(
                        e -> {
                            assertThat(e.tenantId()).isEqualTo(DeviceRegistry.DEFAULT_TENANT);
                            assertThat(e.deviceId()).endsWith("/sys1");
                            assertThat(e.source())
                                    .isEqualTo(TelemetryEnvelope.SourceProtocol.MAVLINK);
                        });

        assertThat(gateway.trackedUdpSenders()).isEqualTo(1);
        assertThat(gateway.counters().publishFailures()).isZero();
    }

    @Test
    @DisplayName("An unregistered sender's datagrams are dropped without state being kept for it")
    void ignoresUnregisteredSender() throws Exception {
        InetSocketAddress address =
                startUdpGateway(new AdmissionController(), DeviceRegistry.closed());

        LoadHarness.sendUdp(address, frames.subList(0, 50), 0);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> assertThat(gateway.counters().connectionsRejected()).isPositive());

        assertThat(publisher.published()).isEmpty();
        // Refusing a sender must not allocate reassembly state for it, or an unauthenticated
        // peer could grow the gateway's memory just by varying its source port.
        assertThat(gateway.trackedUdpSenders()).isZero();
    }

    @Test
    @DisplayName("UDP sheds rather than pausing — there is no back channel to push against")
    void shedsInsteadOfPausing() throws Exception {
        AdmissionController admission = new AdmissionController(32);
        InetSocketAddress address = startUdpGateway(admission, DeviceRegistry.open());

        publisher.stall();
        LoadHarness.sendUdp(address, frames, 0);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(admission.stats().shedTotal()).isPositive());

        // The datagram handler never touches autoRead: pausing a UDP socket does not slow the
        // sender, it just moves the loss into the kernel where it cannot be counted.
        assertThat(gateway.counters().readPauses()).isZero();
        assertThat(admission.stats().shedCritical()).isZero();
        assertThat(admission.stats().shedBulk()).isPositive();
    }
}
