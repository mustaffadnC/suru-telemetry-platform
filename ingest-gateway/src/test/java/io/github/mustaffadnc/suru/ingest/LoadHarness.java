package io.github.mustaffadnc.suru.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

/**
 * Drives the gateway with many simultaneous vehicles replaying a recorded flight.
 *
 * <p>The load is a real ArduPilot SITL capture replayed N times over N connections rather than
 * synthetic frames: it preserves the message mix, the frame-size distribution and the boot banner
 * that a generator would have to guess at. It is <em>replay</em>, not live SITL — the numbers say
 * what the gateway does with realistic bytes, not what a live simulator would do.
 */
public final class LoadHarness {

    /** Result of one load run. */
    public record Result(
            int connections,
            int repeatsPerConnection,
            long bytesSent,
            long framesExpected,
            long elapsedNanos) {

        /**
         * Bytes per second pushed into the gateway.
         *
         * @return throughput in MB/s
         */
        public double megabytesPerSecond() {
            return bytesSent / (elapsedNanos / 1_000_000_000.0) / (1024 * 1024);
        }

        /**
         * Frames per second offered.
         *
         * @return frames per second
         */
        public double framesPerSecond() {
            return framesExpected / (elapsedNanos / 1_000_000_000.0);
        }

        @Override
        public String toString() {
            return "%d conns x%d: %.1f MB/s, %,.0f frames/s, %.2f s"
                    .formatted(
                            connections,
                            repeatsPerConnection,
                            megabytesPerSecond(),
                            framesPerSecond(),
                            elapsedNanos / 1_000_000_000.0);
        }
    }

    private LoadHarness() {
        throw new AssertionError("utility class");
    }

    /**
     * Loads the recorded SITL stream from the test classpath.
     *
     * @return the raw bytes
     */
    public static byte[] recordedStream() {
        try (InputStream in = LoadHarness.class.getResourceAsStream("/mavlink/sitl_stream.bin")) {
            if (in == null) {
                throw new IllegalStateException("sitl_stream.bin missing from the test classpath");
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Splits a MAVLink byte stream into frame-aligned chunks.
     *
     * <p>Needed for UDP, where a real vehicle sends one frame per datagram. Feeding arbitrary byte
     * slices instead would make the test depend on datagrams arriving in order, which UDP does not
     * promise even on loopback.
     *
     * @param stream raw bytes, possibly with leading non-frame garbage
     * @return one byte array per complete frame found
     */
    public static List<byte[]> splitIntoFrames(byte[] stream) {
        List<byte[]> frames = new ArrayList<>();
        int pos = 0;
        while (pos < stream.length) {
            int stx = stream[pos] & 0xFF;
            if (stx == 0xFD && pos + 10 <= stream.length) {
                int payloadLength = stream[pos + 1] & 0xFF;
                int incompatible = stream[pos + 2] & 0xFF;
                int signature = (incompatible & 0x01) != 0 ? 13 : 0;
                int total = 10 + payloadLength + 2 + signature;
                if (pos + total <= stream.length) {
                    byte[] frame = new byte[total];
                    System.arraycopy(stream, pos, frame, 0, total);
                    frames.add(frame);
                    pos += total;
                    continue;
                }
            }
            pos++; // Boot banner and anything else that is not a frame.
        }
        return frames;
    }

    /**
     * Opens {@code connections} TCP sockets and replays the stream on each.
     *
     * @param address the gateway's TCP address
     * @param stream bytes to replay
     * @param connections how many simultaneous senders
     * @param repeats how many times each sender replays the stream
     * @param framesPerStream frames contained in one replay, for the reported rate
     * @return timing and volume of the run
     */
    public static Result runTcp(
            InetSocketAddress address,
            byte[] stream,
            int connections,
            int repeats,
            int framesPerStream) {

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        long start = System.nanoTime();
        try {
            List<CompletableFuture<Void>> senders = new ArrayList<>();
            for (int i = 0; i < connections; i++) {
                senders.add(
                        CompletableFuture.runAsync(
                                () -> replayTcp(address, stream, repeats), pool));
            }
            CompletableFuture.allOf(senders.toArray(CompletableFuture[]::new)).join();
        } finally {
            pool.shutdown();
        }
        long elapsed = System.nanoTime() - start;

        return new Result(
                connections,
                repeats,
                (long) stream.length * connections * repeats,
                (long) framesPerStream * connections * repeats,
                elapsed);
    }

    private static void replayTcp(InetSocketAddress address, byte[] stream, int repeats) {
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(256 * 1024);
            OutputStream out = socket.getOutputStream();
            for (int r = 0; r < repeats; r++) {
                out.write(stream);
            }
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Sends frames as individual datagrams, one frame per packet, as a vehicle would.
     *
     * @param address the gateway's UDP address
     * @param frames frame-aligned chunks
     * @param pauseEveryNPackets insert a brief yield after this many packets, to give the
     *     receiver a chance and keep the kernel's receive buffer from overflowing; {@code 0}
     *     disables it
     */
    public static void sendUdp(InetSocketAddress address, List<byte[]> frames, int pauseEveryNPackets) {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSendBufferSize(1024 * 1024);
            int sent = 0;
            for (byte[] frame : frames) {
                socket.send(
                        new DatagramPacket(frame, frame.length, address.getAddress(), address.getPort()));
                sent++;
                if (pauseEveryNPackets > 0 && sent % pauseEveryNPackets == 0) {
                    Thread.yield();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
