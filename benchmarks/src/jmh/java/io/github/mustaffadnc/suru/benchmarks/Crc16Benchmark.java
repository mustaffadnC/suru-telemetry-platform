package io.github.mustaffadnc.suru.benchmarks;

import io.github.mustaffadnc.suru.protocol.Crc16;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Bitwise versus table-driven CRC.
 *
 * <p>The sizes are the ones that actually occur: 9 bytes is a MAVLink HEARTBEAT payload, 41 is a
 * ÇARGE environment record, 280 is the largest possible MAVLink frame, and 4096 stands in for bulk
 * verification of an archived log. A speedup measured only on megabyte buffers would say nothing
 * about a gateway that checksums a few dozen bytes at a time.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Crc16Benchmark {

    @Param({"9", "41", "280", "4096"})
    public int size;

    private byte[] data;

    @Setup
    public void setUp() {
        data = new byte[size];
        RandomGenerator.of("L64X128MixRandom").nextBytes(data);
    }

    @Benchmark
    public int mcrf4xxBitwise() {
        return Crc16.mcrf4xx(data);
    }

    @Benchmark
    public int mcrf4xxTable() {
        return Crc16.mcrf4xxFast(data);
    }

    @Benchmark
    public int ccittFalseBitwise() {
        return Crc16.ccittFalse(data);
    }

    @Benchmark
    public int ccittFalseTable() {
        return Crc16.ccittFalseFast(data);
    }

}
