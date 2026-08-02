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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connection MAVLink ingest over TCP.
 *
 * <p>One instance per channel, so the decoder's reassembly state and the connection's identity live
 * together and need no synchronisation: everything here runs on that channel's event loop, except
 * the publisher completion callback, which is explicitly bounced back onto it.
 *
 * <p><b>Backpressure on TCP is real.</b> Setting {@code autoRead(false)} stops draining the socket
 * receive buffer; it fills, the window closes, and the sender is told to slow down by TCP itself.
 * Nothing is lost and nobody has to guess. That is why this handler pauses rather than sheds
 * whenever it can — shedding is the fallback for messages that arrive while there is still work to
 * accept, and for UDP, where no such mechanism exists.
 */
public final class MavlinkIngestHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MavlinkIngestHandler.class);

    private final MavlinkDecoder decoder;
    private final AdmissionController admission;
    private final TelemetryPublisher publisher;
    private final DeviceRegistry registry;
    private final GatewayCounters counters;
    private final DuplicateFilter duplicates;

    private String tenantId;
    private String linkId;
    private byte[] scratch = new byte[8192];
    private volatile boolean readPaused;

    /**
     * Creates a handler for one connection.
     *
     * @param dialect MAVLink dialect used to validate frames
     * @param admission shared admission controller
     * @param publisher where admitted telemetry goes
     * @param registry resolves the owning tenant
     * @param counters gateway-wide counters
     * @param duplicates suppresses telemetry already seen
     */
    public MavlinkIngestHandler(
            MavlinkDialect dialect,
            AdmissionController admission,
            TelemetryPublisher publisher,
            DeviceRegistry registry,
            GatewayCounters counters,
            DuplicateFilter duplicates) {
        this.decoder = new MavlinkDecoder(dialect);
        this.admission = admission;
        this.publisher = publisher;
        this.registry = registry;
        this.counters = counters;
        this.duplicates = duplicates;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        var remote = ctx.channel().remoteAddress();
        var tenant = registry.resolveTenant(remote);
        if (tenant.isEmpty()) {
            counters.connectionRejected();
            log.warn("rejecting connection from unregistered peer {}", remote);
            ctx.close();
            return;
        }
        tenantId = tenant.get();
        linkId = ctx.channel().id().asShortText();
        counters.connectionAccepted();
        log.info("link {} open from {} for tenant {}", linkId, remote, tenantId);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf buf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        try {
            int available = buf.readableBytes();
            if (scratch.length < available) {
                scratch = new byte[Math.max(available, scratch.length * 2)];
            }
            buf.readBytes(scratch, 0, available);
            decoder.feed(scratch, 0, available, this::onFrame);
        } finally {
            buf.release();
        }
        updateReadGate(ctx);
    }

    private void onFrame(MavlinkFrame frame) {
        MessagePriority priority = MessagePriority.ofMavlink(frame.messageId());
        if (!admission.tryAdmit(priority)) {
            return; // Shed, and counted as such by the controller.
        }

        // The frame is a view into the decoder's buffer and dies when this returns, so the
        // payload is copied here. This is the one place the flyweight boundary is crossed.
        TelemetryEnvelope envelope =
                buildEnvelope(frame, priority);

        // Deduplication runs after admission so a suppressed duplicate still releases the
        // capacity it reserved; skipping the release here would leak pressure and eventually
        // wedge the gateway shut.
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

    private TelemetryEnvelope buildEnvelope(MavlinkFrame frame, MessagePriority priority) {
        return new TelemetryEnvelope(
                tenantId,
                DeviceRegistry.deviceIdOf(linkId, frame.systemId()),
                TelemetryEnvelope.SourceProtocol.MAVLINK,
                frame.messageId(),
                frame.sequence(),
                frame.systemId(),
                frame.componentId(),
                System.nanoTime(),
                priority,
                frame.copyPayload());
    }

    /**
     * Opens or closes the socket's read gate.
     *
     * <p>Resuming cannot be driven from here alone: pressure falls when a publication completes,
     * which happens on a broker callback thread long after the last read. So a paused channel
     * schedules its own re-check back onto the event loop from that callback.
     */
    private void updateReadGate(ChannelHandlerContext ctx) {
        var config = ctx.channel().config();
        if (config.isAutoRead() && admission.shouldPauseReading()) {
            config.setAutoRead(false);
            readPaused = true;
            counters.readPaused();
            log.debug("link {} pausing reads at pressure {}", linkId, admission.pressure());
            scheduleResume(ctx);
        } else if (!config.isAutoRead() && admission.mayResumeReading()) {
            config.setAutoRead(true);
            readPaused = false;
            log.debug("link {} resuming reads at pressure {}", linkId, admission.pressure());
        }
    }

    private void scheduleResume(ChannelHandlerContext ctx) {
        if (!ctx.channel().isActive()) {
            return;
        }
        ctx.channel()
                .eventLoop()
                .schedule(
                        () -> {
                            if (readPaused && ctx.channel().isActive()) {
                                updateReadGate(ctx);
                                if (readPaused) {
                                    scheduleResume(ctx);
                                }
                            }
                        },
                        5,
                        java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // A closed link means no more bytes are coming: anything the decoder is still holding
        // is a truncated frame, not an incomplete one, and is resynced past so the statistics
        // account for it rather than leaving it dangling.
        decoder.endOfStream(this::onFrame);
        log.info("link {} closed; {}", linkId, decoder.stats());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("link {} failed: {}", linkId, cause.toString());
        ctx.close();
    }

    /**
     * Decoder statistics for this connection.
     *
     * @return the decoder's counters
     */
    public io.github.mustaffadnc.suru.protocol.mavlink.MavlinkStats decoderStats() {
        return decoder.stats();
    }
}
