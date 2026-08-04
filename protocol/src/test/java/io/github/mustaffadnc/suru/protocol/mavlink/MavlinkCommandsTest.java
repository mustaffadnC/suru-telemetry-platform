package io.github.mustaffadnc.suru.protocol.mavlink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** COMMAND_LONG construction and COMMAND_ACK reading. */
class MavlinkCommandsTest {

    private static final MavlinkDialect DIALECT = MavlinkDialect.arduPilotMega();

    /** MAV_CMD_COMPONENT_ARM_DISARM. */
    private static final int MAV_CMD_ARM_DISARM = 400;

    /**
     * The length is not hardcoded on faith: the dialect was generated from the MAVLink definitions,
     * so agreeing with it checks the layout against the protocol rather than against this file.
     */
    @Test
    @DisplayName("the payload length matches what the dialect says COMMAND_LONG is")
    void lengthAgreesWithTheDialect() {
        assertThat(MavlinkCommands.COMMAND_LONG_LENGTH)
                .isEqualTo(DIALECT.maxPayloadLength(MavlinkCommands.MSG_COMMAND_LONG))
                .isEqualTo(33);

        byte[] payload = MavlinkCommands.commandLong(MAV_CMD_ARM_DISARM, 1, 1, 0, 1.0f);
        assertThat(payload).hasSize(33);
    }

    /**
     * Wire order is by descending field size, not declaration order.
     *
     * <p>{@code command} is declared third and transmitted at offset 28, after all seven floats.
     * Writing the struct in declaration order yields a frame that passes its checksum and means
     * something else entirely.
     */
    @Test
    @DisplayName("command sits at offset 28, after the seven floats")
    void wireOrderPutsCommandAfterTheFloats() {
        byte[] payload =
                MavlinkCommands.commandLong(MAV_CMD_ARM_DISARM, 42, 7, 3, 1.0f, 2.0f);

        MavlinkPayload view = MavlinkPayload.of(payload);
        assertThat(view.f32(0)).isEqualTo(1.0f);
        assertThat(view.f32(4)).isEqualTo(2.0f);
        assertThat(view.u16(28)).isEqualTo(MAV_CMD_ARM_DISARM);
        assertThat(view.u8(30)).isEqualTo(42);
        assertThat(view.u8(31)).isEqualTo(7);
        assertThat(view.u8(32)).isEqualTo(3);
    }

    @Test
    @DisplayName("unsupplied parameters are zero")
    void missingParametersAreZero() {
        MavlinkPayload view =
                MavlinkPayload.of(MavlinkCommands.commandLong(21, 1, 1, 0));

        for (int i = 0; i < 7; i++) {
            assertThat(view.f32(i * 4)).as("param%d", i + 1).isZero();
        }
        assertThat(view.u16(28)).isEqualTo(21);
    }

    @Test
    @DisplayName("more than seven parameters is rejected")
    void tooManyParameters() {
        assertThatThrownBy(
                        () ->
                                MavlinkCommands.commandLong(
                                        400, 1, 1, 0, 1, 2, 3, 4, 5, 6, 7, 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("seven parameters");
    }

    @Test
    @DisplayName("a built COMMAND_LONG survives encoding and decoding")
    void roundTripsThroughTheCodec() {
        MavlinkEncoder encoder = new MavlinkEncoder(DIALECT, 255, 190);
        byte[] payload =
                MavlinkCommands.commandLong(MAV_CMD_ARM_DISARM, 1, 1, 0, 1.0f, 0f, 0f, 0f, 0f, 0f, 0f);

        byte[] frame = encoder.encodeV2(MavlinkCommands.MSG_COMMAND_LONG, payload);

        List<MavlinkPayload> decoded = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        MavlinkDecoder decoder = new MavlinkDecoder(DIALECT);
        decoder.feed(
                frame,
                0,
                frame.length,
                f -> {
                    ids.add(f.messageId());
                    decoded.add(MavlinkPayload.of(f.copyPayload()));
                });

        assertThat(ids).containsExactly(MavlinkCommands.MSG_COMMAND_LONG);
        MavlinkPayload view = decoded.getFirst();
        assertThat(view.f32(0)).as("param1 = 1 means arm").isEqualTo(1.0f);
        assertThat(view.u16(28)).isEqualTo(MAV_CMD_ARM_DISARM);
    }

    /**
     * The truncation case a real vehicle produces.
     *
     * <p>COMMAND_ACK is 3 bytes at minimum and 10 with its extension fields, and a v2 sender may
     * send the short form. Both readable fields sit inside the first three bytes, so neither is
     * lost — which is why they are read by offset rather than by unpacking a fixed-size struct.
     */
    @Test
    @DisplayName("a truncated COMMAND_ACK still yields its command and result")
    void ackReadableWhenTruncated() {
        assertThat(DIALECT.minPayloadLength(MavlinkCommands.MSG_COMMAND_ACK)).isEqualTo(3);
        assertThat(DIALECT.maxPayloadLength(MavlinkCommands.MSG_COMMAND_ACK)).isEqualTo(10);

        // The short form: command = 400, result = 0.
        byte[] shortAck = {(byte) 0x90, 0x01, 0x00};
        MavlinkPayload view = MavlinkPayload.of(shortAck);

        assertThat(MavlinkCommands.ackCommand(view)).isEqualTo(MAV_CMD_ARM_DISARM);
        assertThat(MavlinkCommands.ackResult(view)).isEqualTo(MavlinkCommands.RESULT_ACCEPTED);
    }

    @Test
    @DisplayName("a rejected COMMAND_ACK reports its result code")
    void ackCarriesRejection() {
        byte[] rejected = {(byte) 0x90, 0x01, 0x04};

        assertThat(MavlinkCommands.ackResult(MavlinkPayload.of(rejected)))
                .as("MAV_RESULT_FAILED, which is an answer rather than an absence of one")
                .isEqualTo(4);
    }

    /**
     * Pins the namespace confusion that produces a valid frame meaning something else.
     *
     * <p>MAV_CMD 400 is arm/disarm and rides inside COMMAND_LONG. Message id 400 is an unrelated
     * 254-byte message. The dialect answers for both, so nothing downstream catches the mix-up.
     */
    @Test
    @DisplayName("MAV_CMD ids and message ids are different namespaces")
    void commandIdsAreNotMessageIds() {
        assertThat(DIALECT.maxPayloadLength(400))
                .as("message 400 is not arm/disarm and is not 33 bytes")
                .isNotEqualTo(MavlinkCommands.COMMAND_LONG_LENGTH)
                .isEqualTo(254);

        byte[] payload = MavlinkCommands.commandLong(400, 1, 1, 0, 1.0f);
        assertThat(payload).as("arm/disarm is carried, not addressed").hasSize(33);
        assertThat(MavlinkPayload.of(payload).u16(28)).isEqualTo(400);
    }
}
