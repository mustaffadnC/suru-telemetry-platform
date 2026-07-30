package io.github.mustaffadnc.suru.benchmarks;

import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDecoder;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkDialect;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * End-to-end decode throughput over a real recorded ArduPilot SITL stream.
 *
 * <p>36 KB captured off an actual link: 1058 frames across 30 message types, preceded by the
 * autopilot's text boot banner. Using a recording rather than synthetic frames keeps the frame-size
 * mix, the resync at the head and the message variety honest.
 *
 * <p>The {@code chunkSize} parameter models how the bytes arrive. A gateway does not receive whole
 * frames; it receives whatever the socket hands it, and reassembly cost is part of the real cost.
 * {@code 0} means "the whole buffer at once" as an upper bound.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MavlinkDecodeBenchmark {

    /** Bytes handed to the decoder per call; 0 feeds the whole stream in one go. */
    @Param({"0", "64", "1500"})
    public int chunkSize;

    private byte[] stream;
    private MavlinkDecoder decoder;

    @Setup(Level.Trial)
    public void loadStream() {
        try (InputStream in =
                MavlinkDecodeBenchmark.class.getResourceAsStream("/mavlink/sitl_stream.bin")) {
            if (in == null) {
                throw new IllegalStateException(
                        "sitl_stream.bin not on the jmh classpath — check the resources srcDir");
            }
            stream = in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Setup(Level.Invocation)
    public void freshDecoder() {
        decoder = new MavlinkDecoder(MavlinkDialect.arduPilotMega());
    }

    @Benchmark
    public void decodeStream(Blackhole bh) {
        MavlinkDecoder.FrameHandler handler = f -> bh.consume(f.messageId());
        if (chunkSize == 0) {
            decoder.feed(stream, handler);
        } else {
            for (int off = 0; off < stream.length; off += chunkSize) {
                decoder.feed(stream, off, Math.min(chunkSize, stream.length - off), handler);
            }
        }
        decoder.endOfStream(handler);
        bh.consume(decoder.stats().framesDecoded());
    }

    /** Same work, but every frame is copied out — the cost the flyweight design avoids. */
    @Benchmark
    public void decodeStreamCopyingEveryPayload(Blackhole bh) {
        MavlinkDecoder.FrameHandler handler = f -> bh.consume(f.copyPayload());
        if (chunkSize == 0) {
            decoder.feed(stream, handler);
        } else {
            for (int off = 0; off < stream.length; off += chunkSize) {
                decoder.feed(stream, off, Math.min(chunkSize, stream.length - off), handler);
            }
        }
        decoder.endOfStream(handler);
        bh.consume(decoder.stats().framesDecoded());
    }
}
