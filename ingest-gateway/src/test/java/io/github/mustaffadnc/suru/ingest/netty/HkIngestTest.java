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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HkIngestTest {

    /** Records in the phase 1 fixture: META, ENV, IMU, GPS, EVENT. */
    private static final int EXPECTED_RECORDS = 5;

    private static byte[] capsuleLog;

    private TelemetryGateway gateway;
    private InMemoryTelemetryPublisher publisher;

    @BeforeAll
    static void loadFixture() throws IOException {
        try (InputStream in = HkIngestTest.class.getResourceAsStream("/hk/sample.bin")) {
            if (in == null) {
                throw new IllegalStateException("hk/sample.bin missing from the test classpath");
            }
            capsuleLog = in.readAllBytes();
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

    private static void upload(InetSocketAddress address, byte[] data) {
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            OutputStream out = socket.getOutputStream();
            for (int off = 0; off < data.length; off += 64) {
                out.write(data, off, Math.min(64, data.length - off));
            }
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @DisplayName("A recovered capsule log uploads through the same gateway as live telemetry")
    void ingestsCapsuleLog() throws Exception {
        publisher = new InMemoryTelemetryPublisher();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(),
                        new AdmissionController(),
                        publisher,
                        DeviceRegistry.open());
        InetSocketAddress address = gateway.startHk(0);

        upload(address, capsuleLog);

        // The fixture deliberately contains a corrupted frame, a false 'HK' header and a torn
        // tail — the shape a capsule that lost power mid-write leaves behind. All five good
        // records must still come through.
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(
                        () -> assertThat(publisher.published()).hasSize(EXPECTED_RECORDS));

        List<TelemetryEnvelope> envelopes = publisher.published();
        assertThat(envelopes)
                .allSatisfy(
                        e -> {
                            assertThat(e.source())
                                    .isEqualTo(TelemetryEnvelope.SourceProtocol.HK);
                            assertThat(e.tenantId()).isEqualTo(DeviceRegistry.DEFAULT_TENANT);
                            // HK framing carries no sequence or system id; -1 says "absent"
                            // rather than inventing a zero downstream would trust.
                            assertThat(e.sequence()).isEqualTo(-1);
                            assertThat(e.systemId()).isEqualTo(-1);
                        });

        assertThat(envelopes).extracting(TelemetryEnvelope::messageId)
                .containsExactlyInAnyOrder(1, 2, 3, 4, 5);
        assertThat(gateway.counters().publishFailures()).isZero();
    }

    @Test
    @DisplayName("Mission events outrank the 100 Hz inertial stream")
    void prioritisesIrreplaceableRecords() throws Exception {
        publisher = new InMemoryTelemetryPublisher();
        gateway =
                new TelemetryGateway(
                        MavlinkDialect.arduPilotMega(),
                        new AdmissionController(),
                        publisher,
                        DeviceRegistry.open());
        InetSocketAddress address = gateway.startHk(0);

        upload(address, capsuleLog);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(
                        () -> assertThat(publisher.published()).hasSize(EXPECTED_RECORDS));

        // There is one EVENT per state transition in a whole flight and nothing else records
        // when the capsule was released; IMU arrives at 100 Hz and any single sample is
        // disposable. The bands have to reflect that, not the message rate.
        assertThat(priorityOf(publisher.published(), 5)).isEqualTo(MessagePriority.CRITICAL);
        assertThat(priorityOf(publisher.published(), 1)).isEqualTo(MessagePriority.CRITICAL);
        assertThat(priorityOf(publisher.published(), 4)).isEqualTo(MessagePriority.HIGH);
        assertThat(priorityOf(publisher.published(), 3)).isEqualTo(MessagePriority.BULK);
    }

    private static MessagePriority priorityOf(List<TelemetryEnvelope> envelopes, int recordType) {
        return envelopes.stream()
                .filter(e -> e.messageId() == recordType)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no record of type " + recordType))
                .priority();
    }
}
