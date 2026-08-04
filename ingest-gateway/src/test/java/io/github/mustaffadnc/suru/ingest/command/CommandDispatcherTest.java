package io.github.mustaffadnc.suru.ingest.command;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkCommands;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDecoder;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkPayload;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Turning a command into a frame, and what happens when there is nowhere to send it. */
class CommandDispatcherTest {

    private static final MavlinkDialect DIALECT = MavlinkDialect.arduPilotMega();
    private static final int MAV_CMD_ARM_DISARM = 400;
    private static final String TENANT = "acme";
    private static final String DEVICE = "link/sys1";

    /** A link that records what was written to it. */
    private static final class FakeLink {
        private final String name;
        private final List<byte[]> written = new ArrayList<>();
        private boolean accepting = true;

        FakeLink(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private record Decoded(int systemId, int componentId, int messageId, byte[] payload) {}

    private static List<Decoded> decode(byte[] frame) {
        List<Decoded> frames = new ArrayList<>();
        new MavlinkDecoder(DIALECT)
                .feed(
                        frame,
                        0,
                        frame.length,
                        f ->
                                frames.add(
                                        new Decoded(
                                                f.systemId(),
                                                f.componentId(),
                                                f.messageId(),
                                                f.copyPayload())));
        return frames;
    }

    private static CommandDispatcher<FakeLink> dispatcher(DeviceLinkRegistry<FakeLink> registry) {
        return new CommandDispatcher<>(
                registry,
                (link, frame) -> {
                    if (!link.accepting) {
                        return false;
                    }
                    link.written.add(frame);
                    return true;
                },
                DIALECT);
    }

    @Test
    @DisplayName("a command becomes a COMMAND_LONG addressed at the vehicle")
    void buildsAnAddressedCommandLong() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        FakeLink link = new FakeLink("a");
        registry.register(TENANT, DEVICE, 3, 1, link);

        boolean sent =
                dispatcher(registry)
                        .dispatch(
                                new CommandDispatcher.OutboundCommand(
                                        TENANT, DEVICE, MAV_CMD_ARM_DISARM, new float[] {1.0f}));

        assertThat(sent).isTrue();
        assertThat(link.written).hasSize(1);

        Decoded frame = decode(link.written.getFirst()).getFirst();
        assertThat(frame.messageId()).isEqualTo(MavlinkCommands.MSG_COMMAND_LONG);
        assertThat(frame.systemId())
                .as("the frame is from the ground station, not from the vehicle")
                .isEqualTo(CommandDispatcher.GROUND_STATION_SYSTEM_ID);

        MavlinkPayload payload = MavlinkPayload.of(frame.payload());
        assertThat(payload.u16(28)).isEqualTo(MAV_CMD_ARM_DISARM);
        assertThat(payload.f32(0)).as("param1 = 1 arms").isEqualTo(1.0f);
        assertThat(payload.u8(30)).as("target_system is the vehicle's").isEqualTo(3);
        assertThat(payload.u8(31)).isEqualTo(1);
    }

    /**
     * The decision this class documents: an unreachable vehicle does not block the queue.
     *
     * <p>Retrying would stop every command behind it, including commands for vehicles that are
     * connected — one aircraft switched off would make the whole fleet uncommandable. Dropping is
     * safe because the command was already recorded as SENT and will expire into TIMED_OUT, which
     * is exactly what the operator needs to see.
     */
    @Test
    @DisplayName("a command for a vehicle with no link is dropped rather than retried")
    void unreachableVehicleIsDropped() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        CommandDispatcher<FakeLink> dispatcher = dispatcher(registry);

        boolean sent =
                dispatcher.dispatch(
                        new CommandDispatcher.OutboundCommand(
                                TENANT, "link/absent", MAV_CMD_ARM_DISARM, new float[] {1.0f}));

        assertThat(sent).isFalse();
        assertThat(dispatcher.stats().unreachable()).isEqualTo(1);
        assertThat(dispatcher.stats().dispatched()).isZero();
    }

    @Test
    @DisplayName("a link that refuses the write is reported, not counted as sent")
    void refusedWriteIsNotSuccess() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        FakeLink link = new FakeLink("closing");
        link.accepting = false;
        registry.register(TENANT, DEVICE, 1, 1, link);
        CommandDispatcher<FakeLink> dispatcher = dispatcher(registry);

        assertThat(
                        dispatcher.dispatch(
                                new CommandDispatcher.OutboundCommand(
                                        TENANT, DEVICE, 21, new float[0])))
                .isFalse();
        assertThat(dispatcher.stats().writeFailures()).isEqualTo(1);
    }

    /**
     * Sequence numbers belong to a sending endpoint, and one shared counter would make every
     * vehicle see gaps wherever another vehicle's command consumed a number — which is precisely
     * how a receiver counts packet loss.
     */
    @Test
    @DisplayName("each vehicle gets its own sequence counter")
    void sequencesArePerVehicle() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        FakeLink one = new FakeLink("one");
        FakeLink two = new FakeLink("two");
        registry.register(TENANT, "link/sysA", 1, 1, one);
        registry.register(TENANT, "link/sysB", 2, 1, two);
        CommandDispatcher<FakeLink> dispatcher = dispatcher(registry);

        for (int i = 0; i < 3; i++) {
            dispatcher.dispatch(
                    new CommandDispatcher.OutboundCommand(
                            TENANT, "link/sysA", 21, new float[0]));
        }
        dispatcher.dispatch(
                new CommandDispatcher.OutboundCommand(TENANT, "link/sysB", 21, new float[0]));

        // Sequence is byte 4 of a v2 frame.
        assertThat(one.written).hasSize(3);
        assertThat(one.written.get(0)[4]).isEqualTo((byte) 0);
        assertThat(one.written.get(2)[4]).isEqualTo((byte) 2);

        assertThat(two.written).hasSize(1);
        assertThat(two.written.getFirst()[4])
                .as("B's first command is its own sequence 0, not A's 3")
                .isEqualTo((byte) 0);
    }

    @Test
    @DisplayName("a reconnecting vehicle is addressed on its new link")
    void reconnectReplacesTheLink() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        FakeLink oldLink = new FakeLink("old");
        FakeLink newLink = new FakeLink("new");
        registry.register(TENANT, DEVICE, 1, 1, oldLink);
        registry.register(TENANT, DEVICE, 1, 1, newLink);

        dispatcher(registry)
                .dispatch(new CommandDispatcher.OutboundCommand(TENANT, DEVICE, 21, new float[0]));

        assertThat(newLink.written).hasSize(1);
        assertThat(oldLink.written)
                .as("writing to the closed socket would fail where nobody is looking")
                .isEmpty();
    }

    @Test
    @DisplayName("closing a link removes every vehicle behind it")
    void closingALinkRemovesAllItsVehicles() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        FakeLink router = new FakeLink("router");
        registry.register(TENANT, "link/sys1", 1, 1, router);
        registry.register(TENANT, "link/sys2", 2, 1, router);
        registry.register(TENANT, "link/sys3", 3, 1, new FakeLink("other"));

        assertThat(registry.unregister(router))
                .as("one channel can carry a whole airframe behind a MAVLink router")
                .isEqualTo(2);
        assertThat(registry.size()).isEqualTo(1);
        assertThat(registry.find(TENANT, "link/sys1")).isEmpty();
        assertThat(registry.find(TENANT, "link/sys3")).isPresent();
    }

    @Test
    @DisplayName("a device is not addressable under another tenant")
    void registryIsTenantScoped() {
        DeviceLinkRegistry<FakeLink> registry = new DeviceLinkRegistry<>();
        registry.register(TENANT, DEVICE, 1, 1, new FakeLink("a"));

        assertThat(registry.find("rival", DEVICE)).isEmpty();
    }
}
