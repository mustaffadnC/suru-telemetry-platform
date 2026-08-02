package io.github.mustaffadnc.suru.ingest.netty;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.GatewayCounters;
import io.github.mustaffadnc.suru.ingest.MessagePriority;
import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import io.github.mustaffadnc.suru.ingest.TelemetryPublisher;
import io.github.mustaffadnc.suru.ingest.dedup.DuplicateFilter;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDecoder;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MAVLink ingest over UDP.
 *
 * <p><b>There is no backpressure here, and pretending otherwise would be worse than admitting
 * it.</b> A datagram socket has no back channel: declining to read does not slow the sender down,
 * it just lets the kernel discard datagrams once the receive buffer fills — silently, and without a
 * count the application can rely on. A gateway that "applies backpressure" to UDP by pausing reads
 * has arranged for invisible loss and told itself it was being careful.
 *
 * <p>So this handler never touches {@code autoRead}. It keeps reading as fast as it can and relies
 * entirely on {@link AdmissionController} to shed explicitly, trading silent loss for measured loss.
 * That is strictly worse than what TCP gets, and it is the honest option: an operator can act on a
 * shed counter and cannot act on datagrams that vanished inside the kernel. See ADR-0003.
 *
 * <p><b>Per-sender state.</b> One datagram socket serves every vehicle, so reassembly state is kept
 * per source address. That map is bounded and evicts least-recently-used senders — an unbounded map
 * keyed by remote address is a memory leak that any spoofed source can trigger at will.
 *
 * <p>Netty serves one datagram channel from a single event loop, so this runs single-threaded and
 * the map needs no synchronisation.
 */
public final class MavlinkDatagramHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private static final Logger log = LoggerFactory.getLogger(MavlinkDatagramHandler.class);

    /** How many distinct senders keep reassembly state before the oldest is evicted. */
    public static final int MAX_TRACKED_SENDERS = 4096;

    private record Sender(MavlinkDecoder decoder, String tenantId, String linkId) {}

    private final MavlinkDialect dialect;
    private final AdmissionController admission;
    private final TelemetryPublisher publisher;
    private final DeviceRegistry registry;
    private final GatewayCounters counters;
    private final DuplicateFilter duplicates;

    private final Map<InetSocketAddress, Sender> senders =
            new LinkedHashMap<>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<InetSocketAddress, Sender> eldest) {
                    boolean evict = size() > MAX_TRACKED_SENDERS;
                    if (evict) {
                        log.debug("evicting reassembly state for {}", eldest.getKey());
                    }
                    return evict;
                }
            };

    private byte[] scratch = new byte[2048];
    private Sender current;

    /**
     * Creates the handler for a datagram channel.
     *
     * @param dialect MAVLink dialect used to validate frames
     * @param admission shared admission controller
     * @param publisher where admitted telemetry goes
     * @param registry resolves the owning tenant
     * @param counters gateway-wide counters
     * @param duplicates suppresses telemetry already seen
     */
    public MavlinkDatagramHandler(
            MavlinkDialect dialect,
            AdmissionController admission,
            TelemetryPublisher publisher,
            DeviceRegistry registry,
            GatewayCounters counters,
            DuplicateFilter duplicates) {
        this.dialect = dialect;
        this.admission = admission;
        this.publisher = publisher;
        this.registry = registry;
        this.counters = counters;
        this.duplicates = duplicates;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
        InetSocketAddress source = packet.sender();
        Sender sender = senders.get(source);

        if (sender == null) {
            var tenant = registry.resolveTenant(source);
            if (tenant.isEmpty()) {
                counters.connectionRejected();
                return; // Nothing to close on a datagram socket; just ignore it.
            }
            sender =
                    new Sender(
                            new MavlinkDecoder(dialect),
                            tenant.get(),
                            source.getAddress().getHostAddress() + ':' + source.getPort());
            senders.put(source, sender);
            counters.connectionAccepted();
            log.info("udp sender {} attributed to tenant {}", sender.linkId(), sender.tenantId());
        }

        var content = packet.content();
        int available = content.readableBytes();
        if (scratch.length < available) {
            scratch = new byte[Math.max(available, scratch.length * 2)];
        }
        content.readBytes(scratch, 0, available);

        current = sender;
        sender.decoder().feed(scratch, 0, available, this::onFrame);
        current = null;
    }

    private void onFrame(MavlinkFrame frame) {
        Sender sender = current;
        MessagePriority priority = MessagePriority.ofMavlink(frame.messageId());
        if (!admission.tryAdmit(priority)) {
            return; // Shed, and counted — the only lever this transport has.
        }

        TelemetryEnvelope envelope =
                new TelemetryEnvelope(
                        sender.tenantId(),
                        DeviceRegistry.deviceIdOf(sender.linkId(), frame.systemId()),
                        TelemetryEnvelope.SourceProtocol.MAVLINK,
                        frame.messageId(),
                        frame.sequence(),
                        frame.systemId(),
                        frame.componentId(),
                        TelemetryEnvelope.nowEpochNanos(),
                        priority,
                        frame.copyPayload());

        // Duplicates matter more on UDP than on TCP: a datagram can genuinely be delivered twice
        // by the network, which TCP's own sequencing rules out.
        if (duplicates.isDuplicate(envelope)) {
            admission.release();
            return;
        }

        publisher.publish(envelope)
                .whenComplete(
                        (ignored, error) -> {
                            admission.release();
                            if (error != null) {
                                counters.publishFailed();
                            }
                        });
    }

    /**
     * How many senders currently hold reassembly state.
     *
     * @return the count
     */
    public int trackedSenders() {
        return senders.size();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // A datagram channel must not be closed on a single bad packet: doing so would take the
        // whole fleet's ingest down because one sender misbehaved.
        log.warn("udp ingest error: {}", cause.toString());
    }
}
