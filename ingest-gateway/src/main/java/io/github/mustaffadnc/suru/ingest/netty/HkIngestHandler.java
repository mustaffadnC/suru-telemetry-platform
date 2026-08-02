package io.github.mustaffadnc.suru.ingest.netty;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.GatewayCounters;
import io.github.mustaffadnc.suru.ingest.MessagePriority;
import io.github.mustaffadnc.suru.ingest.TelemetryEnvelope;
import io.github.mustaffadnc.suru.ingest.TelemetryPublisher;
import io.github.mustaffadnc.suru.ingest.dedup.DuplicateFilter;
import io.github.mustaffadnc.suru.protocol.hk.HkDecoder;
import io.github.mustaffadnc.suru.protocol.hk.HkRecord;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest of ÇARGE capsule log records over TCP.
 *
 * <p>The capsule writes to an SD card in flight and has no radio, so this path carries recovered
 * logs rather than live telemetry — a ground tool streams a {@code FL_NNNN.BIN} file up after the
 * capsule is retrieved. That changes two things relative to the MAVLink path.
 *
 * <p><b>There is no system id.</b> An HK frame identifies a record type and nothing else, so the
 * device is the link itself. One upload is one capsule.
 *
 * <p><b>End of stream is meaningful.</b> A file ends; a telemetry link merely goes quiet. When the
 * connection closes, {@link HkDecoder#endOfStream} is what distinguishes a torn tail — the shape a
 * capsule that lost power mid-write leaves behind — from a frame still in transit. The resulting
 * statistics are how much of that flight survived, which is worth logging on close.
 */
public final class HkIngestHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(HkIngestHandler.class);

    private final HkDecoder decoder = new HkDecoder();
    private final AdmissionController admission;
    private final TelemetryPublisher publisher;
    private final DeviceRegistry registry;
    private final GatewayCounters counters;
    private final DuplicateFilter duplicates;

    private String tenantId;
    private String linkId;
    private byte[] scratch = new byte[8192];

    /**
     * Creates a handler for one upload connection.
     *
     * @param admission shared admission controller
     * @param publisher where admitted records go
     * @param registry resolves the owning tenant
     * @param counters gateway-wide counters
     * @param duplicates suppresses records already seen
     */
    public HkIngestHandler(
            AdmissionController admission,
            TelemetryPublisher publisher,
            DeviceRegistry registry,
            GatewayCounters counters,
            DuplicateFilter duplicates) {
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
            log.warn("rejecting capsule upload from unregistered peer {}", remote);
            ctx.close();
            return;
        }
        tenantId = tenant.get();
        linkId = ctx.channel().id().asShortText();
        counters.connectionAccepted();
        log.info("capsule upload {} open from {} for tenant {}", linkId, remote, tenantId);
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
            decoder.feed(scratch, 0, available, this::onRecord);
        } finally {
            buf.release();
        }
        updateReadGate(ctx);
    }

    private void onRecord(HkRecord record) {
        MessagePriority priority = MessagePriority.ofHk(record.type());
        if (!admission.tryAdmit(priority)) {
            return;
        }

        TelemetryEnvelope envelope =
                new TelemetryEnvelope(
                        tenantId,
                        linkId,
                        TelemetryEnvelope.SourceProtocol.HK,
                        record.type(),
                        // The HK framing carries no sequence number; -1 says "absent" rather
                        // than inventing a zero that downstream gap detection would trust.
                        -1,
                        -1,
                        -1,
                        System.nanoTime(),
                        priority,
                        record.copyPayload());

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

    private void updateReadGate(ChannelHandlerContext ctx) {
        var config = ctx.channel().config();
        if (config.isAutoRead() && admission.shouldPauseReading()) {
            config.setAutoRead(false);
            counters.readPaused();
            scheduleResume(ctx);
        } else if (!config.isAutoRead() && admission.mayResumeReading()) {
            config.setAutoRead(true);
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
                            if (ctx.channel().isActive() && !ctx.channel().config().isAutoRead()) {
                                updateReadGate(ctx);
                                scheduleResume(ctx);
                            }
                        },
                        5,
                        java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        decoder.endOfStream(this::onRecord);
        log.info("capsule upload {} complete; {}", linkId, decoder.stats());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("capsule upload {} failed: {}", linkId, cause.toString());
        ctx.close();
    }
}
