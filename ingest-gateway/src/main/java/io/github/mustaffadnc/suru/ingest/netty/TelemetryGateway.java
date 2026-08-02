package io.github.mustaffadnc.suru.ingest.netty;

import io.github.mustaffadnc.suru.ingest.AdmissionController;
import io.github.mustaffadnc.suru.ingest.DeviceRegistry;
import io.github.mustaffadnc.suru.ingest.GatewayCounters;
import io.github.mustaffadnc.suru.ingest.TelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TCP telemetry ingest server.
 *
 * <p>Deliberately not a Spring application. This process has one job — take bytes off sockets as
 * fast as they arrive and hand them to Kafka — and its scaling profile is connection count, which
 * is nothing like the control plane's. Keeping it a plain Netty server means no container
 * lifecycle, no request-scoped machinery and no reflective startup between the socket and the
 * decoder. See ADR-0002.
 *
 * <p>Bind to port {@code 0} to take an ephemeral port; {@link #localAddress()} then reports what
 * was actually assigned, which is how tests avoid fighting over fixed ports.
 */
public final class TelemetryGateway implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TelemetryGateway.class);

    private final MavlinkDialect dialect;
    private final AdmissionController admission;
    private final TelemetryPublisher publisher;
    private final DeviceRegistry registry;
    private final GatewayCounters counters = new GatewayCounters();

    private EventLoopGroup acceptGroup;
    private EventLoopGroup ioGroup;
    private Channel serverChannel;
    private Channel datagramChannel;
    private MavlinkDatagramHandler datagramHandler;

    /**
     * Creates a gateway.
     *
     * @param dialect MAVLink dialect used to validate frames
     * @param admission admission controller shared across connections
     * @param publisher where admitted telemetry goes
     * @param registry resolves the owning tenant for a connection
     */
    public TelemetryGateway(
            MavlinkDialect dialect,
            AdmissionController admission,
            TelemetryPublisher publisher,
            DeviceRegistry registry) {
        this.dialect = dialect;
        this.admission = admission;
        this.publisher = publisher;
        this.registry = registry;
    }

    /**
     * Binds and begins accepting connections.
     *
     * @param port TCP port, or {@code 0} for an ephemeral one
     * @return the address actually bound
     * @throws InterruptedException if the bind is interrupted
     * @throws IllegalStateException if already started
     */
    public InetSocketAddress start(int port) throws InterruptedException {
        if (serverChannel != null) {
            throw new IllegalStateException("gateway already started");
        }
        ensureGroups();

        ServerBootstrap bootstrap =
                new ServerBootstrap()
                        .group(acceptGroup, ioGroup)
                        .channel(NioServerSocketChannel.class)
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        // Telemetry frames are small and latency matters more than packing
                        // them, so Nagle's algorithm is off.
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        .childHandler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel ch) {
                                        ch.pipeline()
                                                .addLast(
                                                        new MavlinkIngestHandler(
                                                                dialect,
                                                                admission,
                                                                publisher,
                                                                registry,
                                                                counters));
                                    }
                                });

        serverChannel = bootstrap.bind(port).sync().channel();
        InetSocketAddress bound = (InetSocketAddress) serverChannel.localAddress();
        log.info("telemetry gateway listening on {}", bound);
        return bound;
    }

    /**
     * Binds a datagram socket and begins accepting telemetry on it.
     *
     * <p>Unlike the TCP listener this exerts no backpressure, because UDP offers none to exert:
     * the handler keeps reading and relies on shedding. See {@link MavlinkDatagramHandler} and
     * ADR-0003.
     *
     * @param port UDP port, or {@code 0} for an ephemeral one
     * @return the address actually bound
     * @throws InterruptedException if the bind is interrupted
     * @throws IllegalStateException if UDP is already bound
     */
    public InetSocketAddress startUdp(int port) throws InterruptedException {
        if (datagramChannel != null) {
            throw new IllegalStateException("udp listener already started");
        }
        ensureGroups();

        datagramHandler =
                new MavlinkDatagramHandler(dialect, admission, publisher, registry, counters);

        Bootstrap bootstrap =
                new Bootstrap()
                        .group(ioGroup)
                        .channel(NioDatagramChannel.class)
                        // A telemetry link bursts; a larger receive buffer absorbs bursts that
                        // would otherwise be dropped by the kernel before the gateway ever
                        // sees them — the loss this transport cannot count.
                        .option(ChannelOption.SO_RCVBUF, 4 * 1024 * 1024)
                        .option(ChannelOption.SO_REUSEADDR, true)
                        .handler(datagramHandler);

        datagramChannel = bootstrap.bind(port).sync().channel();
        InetSocketAddress bound = (InetSocketAddress) datagramChannel.localAddress();
        log.info("telemetry gateway listening for datagrams on {}", bound);
        return bound;
    }

    private void ensureGroups() {
        if (ioGroup != null) {
            return;
        }
        // Netty 4.2's event loop API: NioEventLoopGroup is deprecated in favour of an
        // IoHandler-based group.
        acceptGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ioGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    }

    /**
     * The bound TCP address.
     *
     * @return the address, or {@code null} if TCP is not started
     */
    public InetSocketAddress localAddress() {
        return serverChannel == null ? null : (InetSocketAddress) serverChannel.localAddress();
    }

    /**
     * How many UDP senders currently hold reassembly state.
     *
     * @return the count, or {@code 0} if UDP is not started
     */
    public int trackedUdpSenders() {
        return datagramHandler == null ? 0 : datagramHandler.trackedSenders();
    }

    /**
     * Gateway-wide counters: connections, read pauses, publication failures.
     *
     * @return the live counters
     */
    public GatewayCounters counters() {
        return counters;
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        if (datagramChannel != null) {
            datagramChannel.close().syncUninterruptibly();
            datagramChannel = null;
            datagramHandler = null;
        }
        if (acceptGroup != null) {
            acceptGroup.shutdownGracefully().syncUninterruptibly();
            acceptGroup = null;
        }
        if (ioGroup != null) {
            ioGroup.shutdownGracefully().syncUninterruptibly();
            ioGroup = null;
        }
        log.info("telemetry gateway stopped");
    }
}
