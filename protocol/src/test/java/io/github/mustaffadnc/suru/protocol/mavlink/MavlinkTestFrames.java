package io.github.mustaffadnc.suru.protocol.mavlink;

import io.github.mustaffadnc.suru.protocol.Crc16;

/**
 * Builds MAVLink frames for tests.
 *
 * <p>Written independently of the decoder rather than by reusing its constants: an encoder that
 * shares the decoder's idea of the layout would agree with it even when both are wrong.
 */
final class MavlinkTestFrames {

    private MavlinkTestFrames() {
        throw new AssertionError("utility class");
    }

    /**
     * Builds a MAVLink v2 frame.
     *
     * @param dialect supplies the CRC_EXTRA for {@code messageId}
     * @param sequence sequence number
     * @param systemId sending system
     * @param componentId sending component
     * @param messageId message id, must be known to {@code dialect}
     * @param payload payload bytes
     * @return the framed bytes
     */
    static byte[] v2(
            MavlinkDialect dialect,
            int sequence,
            int systemId,
            int componentId,
            int messageId,
            byte[] payload) {
        int crcExtra = dialect.crcExtra(messageId);
        if (crcExtra == MavlinkDialect.UNKNOWN) {
            throw new IllegalArgumentException("dialect does not define message " + messageId);
        }
        return v2Raw(sequence, systemId, componentId, messageId, payload, crcExtra, false);
    }

    /**
     * Builds a v2 frame with an explicit CRC_EXTRA, so callers can construct frames for message
     * ids the dialect does not know.
     *
     * @param sequence sequence number
     * @param systemId sending system
     * @param componentId sending component
     * @param messageId message id
     * @param payload payload bytes
     * @param crcExtra the CRC_EXTRA byte to fold in
     * @param signed whether to append a (dummy) signature block
     * @return the framed bytes
     */
    static byte[] v2Raw(
            int sequence,
            int systemId,
            int componentId,
            int messageId,
            byte[] payload,
            int crcExtra,
            boolean signed) {
        int signatureLength = signed ? MavlinkFrame.SIGNATURE_LENGTH : 0;
        byte[] out = new byte[10 + payload.length + 2 + signatureLength];
        out[0] = (byte) MavlinkFrame.STX_V2;
        out[1] = (byte) payload.length;
        out[2] = (byte) (signed ? MavlinkFrame.INCOMPAT_FLAG_SIGNED : 0);
        out[3] = 0;
        out[4] = (byte) sequence;
        out[5] = (byte) systemId;
        out[6] = (byte) componentId;
        out[7] = (byte) messageId;
        out[8] = (byte) (messageId >>> 8);
        out[9] = (byte) (messageId >>> 16);
        System.arraycopy(payload, 0, out, 10, payload.length);

        int crc = Crc16.mcrf4xx(Crc16.INIT, out, 1, 9 + payload.length);
        crc = Crc16.mcrf4xxUpdate(crc, (byte) crcExtra);
        out[10 + payload.length] = (byte) crc;
        out[11 + payload.length] = (byte) (crc >>> 8);
        return out;
    }

    /**
     * Builds a MAVLink v1 frame.
     *
     * @param dialect supplies the CRC_EXTRA for {@code messageId}
     * @param sequence sequence number
     * @param systemId sending system
     * @param componentId sending component
     * @param messageId message id, must fit in one byte
     * @param payload payload bytes
     * @return the framed bytes
     */
    static byte[] v1(
            MavlinkDialect dialect,
            int sequence,
            int systemId,
            int componentId,
            int messageId,
            byte[] payload) {
        int crcExtra = dialect.crcExtra(messageId);
        if (crcExtra == MavlinkDialect.UNKNOWN) {
            throw new IllegalArgumentException("dialect does not define message " + messageId);
        }
        byte[] out = new byte[6 + payload.length + 2];
        out[0] = (byte) MavlinkFrame.STX_V1;
        out[1] = (byte) payload.length;
        out[2] = (byte) sequence;
        out[3] = (byte) systemId;
        out[4] = (byte) componentId;
        out[5] = (byte) messageId;
        System.arraycopy(payload, 0, out, 6, payload.length);

        int crc = Crc16.mcrf4xx(Crc16.INIT, out, 1, 5 + payload.length);
        crc = Crc16.mcrf4xxUpdate(crc, (byte) crcExtra);
        out[6 + payload.length] = (byte) crc;
        out[7 + payload.length] = (byte) (crc >>> 8);
        return out;
    }

    /** Concatenates byte arrays. */
    static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
