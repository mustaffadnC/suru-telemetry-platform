package io.github.mustaffadnc.suru.ingest.command;

import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkCommands;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkEncoder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a command message into a COMMAND_LONG frame and writes it to the vehicle.
 *
 * <h2>What happens when the vehicle is not connected</h2>
 *
 * <p>The message is dropped, counted, and logged — not retried. Retrying would be the instinct, and
 * it is wrong here for the same reason the notifier gives up: this consumer is ordered, so a
 * command for an unreachable vehicle would block every command behind it, including commands for
 * vehicles that are perfectly reachable. One aircraft being switched off would stop the whole fleet
 * being commandable.
 *
 * <p>Dropping is safe because it is <em>visible</em>: the command was recorded as {@code SENT} by
 * the relay and nothing will acknowledge it, so it expires into {@code TIMED_OUT} and the operator
 * sees exactly what happened. That state exists for this case.
 *
 * <h2>One encoder per vehicle</h2>
 *
 * <p>MAVLink sequence numbers belong to a sending endpoint, and this gateway is one endpoint per
 * vehicle it talks to. A single shared counter would make every vehicle see gaps wherever another
 * vehicle's command had consumed a number, and gaps are how a receiver counts packet loss.
 *
 * @param <L> the link type
 */
public final class CommandDispatcher<L> {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    /** The gateway's own MAVLink system id. 255 is the conventional ground-station id. */
    public static final int GROUND_STATION_SYSTEM_ID = 255;

    /** The gateway's component id. 190 is MAV_COMP_ID_MISSIONPLANNER. */
    public static final int GROUND_STATION_COMPONENT_ID = 190;

    /** Writes an encoded frame to a link. */
    @FunctionalInterface
    public interface FrameWriter<L> {
        /**
         * Writes a frame.
         *
         * @param link where to write
         * @param frame the encoded MAVLink frame
         * @return {@code true} if the write was accepted
         */
        boolean write(L link, byte[] frame);
    }

    /**
     * One command to send.
     *
     * @param tenantId owning tenant
     * @param deviceId the vehicle
     * @param mavCommandId the MAV_CMD id
     * @param params the seven command parameters, in order
     */
    public record OutboundCommand(
            String tenantId, String deviceId, int mavCommandId, float[] params) {}

    private final DeviceLinkRegistry<L> registry;
    private final FrameWriter<L> writer;
    private final MavlinkDialect dialect;

    /** One encoder per vehicle, so each sees a coherent sequence. */
    private final Map<String, MavlinkEncoder> encoders = new ConcurrentHashMap<>();

    private final LongAdder dispatched = new LongAdder();
    private final LongAdder unreachable = new LongAdder();
    private final LongAdder writeFailures = new LongAdder();

    /**
     * Creates a dispatcher.
     *
     * @param registry where devices are reachable
     * @param writer how to write to a link
     * @param dialect supplies CRC_EXTRA for the frames built here
     */
    public CommandDispatcher(
            DeviceLinkRegistry<L> registry, FrameWriter<L> writer, MavlinkDialect dialect) {
        this.registry = registry;
        this.writer = writer;
        this.dialect = dialect;
    }

    /**
     * Sends one command.
     *
     * @param command what to send
     * @return {@code true} if a frame reached the link
     */
    public boolean dispatch(OutboundCommand command) {
        Optional<DeviceLinkRegistry.Link<L>> link =
                registry.find(command.tenantId(), command.deviceId());
        if (link.isEmpty()) {
            unreachable.increment();
            log.warn(
                    "no open link for {}/{}; MAV_CMD {} will expire unacknowledged",
                    command.tenantId(),
                    command.deviceId(),
                    command.mavCommandId());
            return false;
        }

        DeviceLinkRegistry.Link<L> target = link.get();
        byte[] payload =
                MavlinkCommands.commandLong(
                        command.mavCommandId(),
                        target.systemId(),
                        target.componentId(),
                        0,
                        command.params());

        MavlinkEncoder encoder =
                encoders.computeIfAbsent(
                        command.tenantId() + '/' + command.deviceId(),
                        key ->
                                new MavlinkEncoder(
                                        dialect,
                                        GROUND_STATION_SYSTEM_ID,
                                        GROUND_STATION_COMPONENT_ID));

        byte[] frame = encoder.encodeV2(MavlinkCommands.MSG_COMMAND_LONG, payload);

        if (!writer.write(target.link(), frame)) {
            writeFailures.increment();
            log.warn("write rejected for {}/{}", command.tenantId(), command.deviceId());
            return false;
        }
        dispatched.increment();
        return true;
    }

    /**
     * Dispatcher counters.
     *
     * @return a snapshot
     */
    public DispatchStats stats() {
        return new DispatchStats(dispatched.sum(), unreachable.sum(), writeFailures.sum());
    }

    /**
     * What the dispatcher has done.
     *
     * @param dispatched frames written to a link
     * @param unreachable commands for vehicles with no open link, which will time out
     * @param writeFailures links that refused the write
     */
    public record DispatchStats(long dispatched, long unreachable, long writeFailures) {}
}
