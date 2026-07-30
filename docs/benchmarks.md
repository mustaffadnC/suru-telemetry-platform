# Benchmarks and measurements

Every performance claim in this repository lands here with the numbers behind it, the hardware it
ran on, and the command that reproduces it. A claim without a number in this file is not a claim.

## Reference machine

| | |
|---|---|
| CPU | AMD Ryzen 5 7600X, 6 cores / 12 threads |
| Memory | 15 GiB available to the runtime |
| OS | Ubuntu 26.04 LTS on WSL2, kernel 6.18 |
| JDK | OpenJDK 25.0.3 |
| Storage | ext4 on NVMe (WSL virtual disk — *not* `/mnt/c`) |

Numbers from a laptop under WSL are not datacenter numbers. They are recorded to compare
alternatives against each other on identical hardware, not to advertise absolute throughput.

JMH settings throughout: 2 forks, 3 warmup iterations, 4 measurement iterations, 1 s each.
Two forks because a single JVM can get lucky with JIT compilation and call it a result.

```bash
./gradlew :benchmarks:jmh
```

## Phase 0 — environment baseline

Measured 29 Jul 2026.

| Check | Result |
|---|---|
| Full infrastructure start (`docker compose up -d`, warm images) | all six services healthy in ~5 s |
| Idle memory footprint of the whole stack | 2.2 GiB |
| TimescaleDB extension | 2.29.0 |

## Phase 1 — protocol core

Measured 31 Jul 2026.

### CRC: bitwise versus table-driven

The bitwise loop is the definition — it reads like the reference C and can be checked against it by
eye. The table is the optimisation. `Crc16Test` proves they agree across the entire byte space and
over random buffers of every length up to 300, so the table cannot silently drift.

Sizes are the ones that actually occur: 9 bytes is a MAVLink HEARTBEAT payload, 41 a ÇARGE
environment record, 280 the largest possible MAVLink frame, 4096 a bulk verification pass over an
archived log.

| Bytes | MCRF4XX bitwise | MCRF4XX table | Gain | CCITT-FALSE bitwise | CCITT-FALSE table | Gain |
|---:|---:|---:|---:|---:|---:|---:|
| 9 | 38.60 ns | 7.49 ns | **5.2×** | 31.76 ns | 9.86 ns | **3.2×** |
| 41 | 201.0 ns | 52.19 ns | **3.9×** | 168.2 ns | 65.13 ns | **2.6×** |
| 280 | 1408 ns | 447.1 ns | **3.1×** | 1230 ns | 503.8 ns | **2.4×** |
| 4096 | 20.63 µs | 6.59 µs | **3.1×** | 19.33 µs | 7.51 µs | **2.6×** |

All figures average time per operation, ±error within 3 % at every point.

The gain is largest on the smallest input, which is the opposite of the usual intuition about lookup
tables and is the reason the small sizes were measured at all: at 9 bytes the bitwise version pays
72 branch-heavy iterations against the table's 9 array reads, and there is no loop long enough for
the branch predictor to amortise them.

### Decoder throughput

Decoding the 36 176-byte recorded ArduPilot SITL stream: 1058 frames across 30 message types,
preceded by 119 bytes of text boot banner that has to be resynced past.

`chunkSize` models how bytes arrive off a socket. A gateway never receives whole frames, and the
cost of reassembling frames that straddle reads is part of the real cost.

| Input chunking | Bitwise CRC | Table CRC | Gain | Throughput | Frame rate |
|---|---:|---:|---:|---:|---:|
| whole buffer | 186.8 µs | **63.66 µs** | 2.93× | 568 MB/s | 16.6 M frames/s |
| 1500 bytes | 188.6 µs | **61.26 µs** | 3.08× | 591 MB/s | 17.3 M frames/s |
| 64 bytes | 190.0 µs | **67.46 µs** | 2.82× | 536 MB/s | 15.7 M frames/s |

The whole decoder got ~3× faster from a change confined to the checksum, which is the measurement
that mattered: **checksumming was very nearly the entire cost of decoding.** Everything else —
scanning for start bytes, bounds checks, dispatch, sequence bookkeeping — was already in the noise.
Optimising any of it first would have bought nothing.

**Reassembly is nearly free.** Feeding the stream 64 bytes at a time costs about 6 % more than
handing over the whole buffer, and 1500-byte chunks are indistinguishable from it. Frames split
across reads — including splits landing mid-header and mid-checksum — do not meaningfully penalise
the decoder.

### What the zero-copy design is actually worth

The frame handed to a callback is a mutable view into the decoder's own buffer, so decoding
allocates nothing per frame. The benchmark that copies every payload out measures what that buys —
and the answer changed once the CRC stopped dominating:

| Input chunking | Cost of copying, bitwise CRC | Cost of copying, table CRC |
|---|---:|---:|
| whole buffer | 2.3 % | **10.4 %** |
| 1500 bytes | 1.3 % | **15.6 %** |
| 64 bytes | 4.4 % | **17.2 %** |

Nothing about the copying changed between those two columns. The *same* copies went from looking
negligible to costing a sixth of the run, purely because the checksum no longer hid them — Amdahl's
law, observed rather than recited. Had the flyweight been evaluated only against the first column it
would have looked like premature optimisation and been a reasonable thing to remove.

Even 10–17 % understates it, because a microbenchmark over one 36 KB stream never reaches the state
that actually punishes allocation: sustained ingest, where the cost surfaces as GC pressure rather
than as time in the decode loop. That measurement belongs in phase 6 with GC instrumentation, and is
listed below. What is claimed here is only what was measured.

## Planned measurements

| Phase | Question | Method |
|---|---|---|
| 2 | Sustained ingest rate before backpressure engages; behaviour when Kafka stalls | synthetic load + SITL fleet |
| 3 | Query p95 across time ranges; disk before/after compression | ≥100M rows loaded |
| 3 | TimescaleDB vs QuestDB ingest and query on identical data | feeds ADR-0004 |
| 4 | Latency from telemetry event to alert delivery | end-to-end timestamps |
| 6 | ZGC vs G1 under ingest load; p99 pause times | JFR + async-profiler |
| 6 | Allocation rate with and without the flyweight, under sustained ingest | GC logs |
| 6 | Recovery time and data loss when Kafka or the database is killed | chaos runs |
