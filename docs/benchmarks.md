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

## Phase 2 — ingest gateway

Measured 2 Aug 2026.

```bash
./gradlew :ingest-gateway:test --rerun -Dsuru.loadtest=true -Dsuru.loadtest.connections=32 -Dsuru.loadtest.repeats=20 --tests '*GatewayLoadTest*'
```

32 simultaneous TCP connections, each replaying the recorded SITL flight 20 times: 677,120 frames,
22.1 MB, into an in-memory publisher (Kafka's own throughput is a separate question and belongs to
phase 6's load work).

| | |
|---|---:|
| Frames ingested | 677,120 |
| Wall time | 0.47 s |
| **Throughput** | **1,437,289 frames/s · 46.9 MB/s** |
| Peak in-flight publications | 20 (capacity 262,144) |
| Shed | 0 |
| Read pauses | 0 |
| Publish failures | 0 |

Nothing was shed and reads never paused, which is the expected result when the downstream keeps up:
admission control should be invisible until it is needed.

### The first measurement was wrong by a factor of 850

The initial run reported **1,692 frames/s**. That number was nonsense, and the way it was nonsense
is worth keeping:

The decoder alone had already been measured at 16.6 M frames/s, so a gateway wrapping it could not
plausibly be four orders of magnitude slower. The cause was `InMemoryTelemetryPublisher` collecting
into a `CopyOnWriteArrayList`, whose `add` copies the entire backing array — 677k appends is on the
order of 10¹¹ element copies. **The load test was measuring the test double, not the system under
test.** Swapping in a `ConcurrentLinkedQueue` and counting rather than retaining took the same run
from 7 min 01 s to 24 s.

The lesson generalises past this one bug: a harness sits on the hot path exactly like production
code does, and a number that disagrees with a component measurement by orders of magnitude is
evidence about the harness before it is evidence about the system.

A second distortion was fixed at the same time — with no Logback configuration present, the default
level is DEBUG, so Netty logged its platform probe and logging sat on the ingest path. Tests now pin
WARN via `logback-test.xml`.

## Phase 3 — storage at a hundred million rows

Measured 4 Aug 2026. 50 devices × 20 metrics = 1,000 series, one sample per second, 100,000,000
rows spanning 27.8 hours across 28 one-hour chunks.

```bash
./gradlew :storage:test --rerun -Dsuru.dbbench=true --tests '*ScaleMeasurementIT*'
```

The measurement was run twice: once on the schema as originally written, and once after
`V4__rollup_indexes.sql` — the migration that came out of the rollup investigation below. Both runs
are kept, because the pair says more than either alone.

| | before `V4` | **after `V4`** |
|---|---:|---:|
| Ingest (`COPY`, 500k batches) | 112,489 rows/s (889 s) | **102,778 rows/s** (973 s) |
| Uncompressed | 22.46 GB, 28 chunks | 22.46 GB, 28 chunks |
| Compressed | 0.84 GB — **26.7×**, in 210 s | 0.84 GB — **26.7×**, in 248 s |
| Rollup refresh, both levels | 41 s | 57 s |
| 1-minute rollup footprint | 0.25 GB (30.3 % of compressed raw) | **0.44 GB (52.5 %)** |

Ingest held its rate from the first batch to the hundred-millionth in both runs — within 2 % between
10M and 100M — so nothing degrades as the table grows.

### Query cost

| Query | p50 | p95 | p95 before `V4` |
|---|---:|---:|---:|
| raw: one series, 1 hour (before compression) | 1.2 ms | 2.0 ms | 1.6 ms |
| raw: one series, 1 hour (after compression) | **0.6 ms** | **1.0 ms** | 0.7 ms |
| raw: one series, whole range, 5-minute buckets | 13.6 ms | 14.8 ms | 10.1 ms |
| **1m rollup: one series, 24 hours** | **1.3 ms** | **2.3 ms** | **70.9 ms** |
| 1h rollup: one series, whole range | 1.0 ms | 1.6 ms | 1.4 ms |
| latest value per metric, one device | 3.4 ms | 5.0 ms | 3.0 ms |
| one metric across all 50 devices, 1h rollup | 2.6 ms | 3.2 ms | 1.8 ms |

### Reading the two runs honestly

One row improved by **31×**. Every other row got 15–65 % *worse*, and the ingest rate fell 8.6 %.
An index on the minute rollup cannot slow down a point query against the raw hypertable — different
table, different access path — so the second column is not measuring a regression the migration
caused.

What pins that down is the quantities that are not timings. Uncompressed size, chunk count,
compression ratio and compressed size came back **bit-identical** across both runs: same data, same
layout, same work. Only the clock moved. The uniform direction across unrelated operations is the
signature of a busier machine, not of a code change — this is a laptop under WSL, and the second run
followed a 100M-row QuestDB load through the same disk.

That is worth stating rather than smoothing over, because it calibrates every timing in this file:
**these numbers carry roughly ±30 % run-to-run spread, and differences smaller than that are not
findings.** The rollup's 31× is far outside it. The 8.6 % ingest difference is not, and no claim is
made from it.

**The index has a real price, and it is visible in the deterministic column.** The minute rollup
grew from 0.25 GB to 0.44 GB — the index is about 76 % the size of the aggregate it indexes, on an
aggregate that was already 30 % of the compressed raw table. That is the actual cost of the 31×, and
it is a trade rather than a free win: this schema now spends roughly half its compressed footprint on
the minute rollup and its index. It is worth it here because the alternative was a rollup slower than
the raw data it summarises, which is a rollup with no reason to exist.

### Compression made reads faster, not slower

The obvious expectation is that compressed data reads slower — decompression is work that
uncompressed data does not do. It measured the other way: p95 on the same query fell from 1.6 ms to
0.7 ms, **a bit over twice as fast after compressing**.

The reason is that the query was never CPU-bound. Segmented by `(tenant, device, metric)`, the rows
a query wants are contiguous and 26× smaller, so the read touches a fraction of the pages it used
to. Decompressing a few kilobytes costs less than fetching a few hundred. Compression is usually
argued for on storage cost; here it paid for itself twice over on latency, which is the stronger
argument and would have been missed by measuring only the footprint.

## Phase 3 — TimescaleDB against QuestDB

Measured 4 Aug 2026, same machine, same 100,000,000 rows, same shape, **run one after the other**
— two databases saturating one disk at once would measure the contention rather than either engine.

QuestDB is fed through its own line protocol rather than its PostgreSQL wire compatibility, and its
dimensions are `SYMBOL` (its interned string type) rather than `VARCHAR`. Comparing a vendor's fast
path against another vendor's slow one produces a number that says nothing.

The TimescaleDB column is the **post-`V4` run**, for two reasons: it is the schema that ships, and it
is the run temporally adjacent to the QuestDB load. Pairing QuestDB against the earlier, faster
TimescaleDB run would have compared across an eight-hour gap in machine conditions — and would have
flattered TimescaleDB on every row, which is exactly the kind of accident that produces a benchmark
confirming whatever the author already decided.

| | TimescaleDB 2.29 | QuestDB 9.4.3 | |
|---|---:|---:|---|
| Ingest | 102,778 rows/s | **1,599,460 rows/s** | QuestDB **15.6×** |
| Wall time for 100M rows | 973 s | **62.5 s** | |
| Footprint | **0.84 GB** (8.4 B/row) | 3.99 GB (39.9 B/row) | TimescaleDB **4.8×** |
| Point query: one series, 1 hour | **1.0 ms** p95 | 19.2 ms p95 | TimescaleDB **19×** |
| Latest value per metric, one device | **5.0 ms** p95 | 60.8 ms p95 | TimescaleDB **12×** |
| One metric across 50 devices | **3.2 ms** p95 ‡ | 84.4 ms p95 | TimescaleDB **26×** |
| One series, 5-min buckets, 24 h | **2.3 ms** p95 ‡ | 46.7 ms p95 | TimescaleDB **20×** |
| One series, 6-h buckets, whole range | **1.6 ms** p95 ‡ | 91.5 ms p95 | TimescaleDB **57×** |

‡ TimescaleDB reads a pre-computed rollup on these rows; QuestDB scans raw with `SAMPLE BY`. **They
are not like-for-like** and are marked because the difference is the point: continuous aggregates are
a feature TimescaleDB has and this QuestDB version does not, so the comparison is "what each system
offers for this workload" rather than "the same work". Ingest, footprint and the point query are
the like-for-like rows.

**Two things changed when the honest column was substituted, and they point opposite ways.** QuestDB's
ingest lead grew from 14.2× to 15.6× and TimescaleDB's query leads all shrank — the case for QuestDB
is stronger against this column than against the one it replaced. But the 5-minute-bucket row, the
single query QuestDB had won at 1.5×, **flipped to a 20× loss** once the rollup carried its index.
After `V4` TimescaleDB is ahead on every query measured and behind only on ingest.

The three like-for-like rows disagree with each other, which is what makes this a decision rather
than a lookup. QuestDB ingests an order of magnitude faster; TimescaleDB stores the same data five
times smaller and answers a point query nineteen times quicker.

ADR-0005 works through what that means for this platform.

### The minute rollup was the slowest path measured — and that was backwards

Before `V4`, at 70.9 ms p95, it was an order of magnitude slower than reading the raw table over the
same series, and slower than the hourly rollup over a range 28 times wider. Rollups exist to be
*cheaper* than raw; that one was not. The cause turned out to be a missing index — but the first
explanation was wrong, and how it was wrong is the useful part.

**Hypothesis 1: real-time aggregation.** A continuous aggregate can union materialised buckets with
a live aggregate over raw data the materialisation has not reached, and that live branch scans the
hypertable. Plausible, widely documented, and false here: `materialized_only` turns out to default
to **true** on TimescaleDB 2.29, so the union never existed. Toggling it changed p95 from 8.6 ms to
8.3 ms — nothing. The test asserting the default was `false` failed, which is how the hypothesis
died. Had the assertion been softer, the wrong explanation would have survived.

**What the numbers actually said.** The same query costs 8.6 ms at ten million rows and 70.9 ms at a
hundred million — roughly one eighth the cost for roughly one eighth of the buckets. Cost tracking
*total data* rather than *the requested series* is what a missing index looks like.

**Hypothesis 2: the rollup has no composite index.** The raw table has carried
`(tenant_id, device_id, metric, time DESC)` since the first migration. The aggregates did not.
TimescaleDB does index them automatically — but one index per `GROUP BY` column:
`(tenant_id, bucket)`, `(device_id, bucket)`, `(metric, bucket)`. A query filtering all three can
use exactly one and then filters the rest.

The controlled test toggles only the index, on the same data in the same run:

| One series, full range | p95 |
|---|---:|
| rollup as created, 10M rows | 8.0 ms |
| rollup with `(tenant_id, device_id, metric, bucket DESC)`, 10M rows | **1.7 ms** |

**4.8× from one index**, and the fix is now migration `V4__rollup_indexes.sql` with a schema test
asserting both rollups carry it.

**The re-run at a hundred million confirmed it, and the confirmation is stronger than the controlled
test.** The same query went from 70.9 ms to 2.3 ms p95 — **31×**, against 4.8× at a tenth the data.
A gain that grows with table size is the specific prediction the missing-index diagnosis makes and
that the real-time-aggregation one does not: an unindexed scan costs more as the table grows, an
indexed lookup barely notices. The hypothesis was not merely consistent with the fix, it was right
about how the fix would scale.

The omission was easy to make for a specific reason worth remembering: continuous aggregates are
addressed as *views*, so they look like they inherit the underlying table's access paths. They do
not. Each is backed by its own hypertable with its own indexes, and indexing the raw table does
nothing for them.

## Planned measurements

| Phase | Question | Method |
|---|---|---|
| 2 | Sustained ingest rate before backpressure engages; behaviour when Kafka stalls | synthetic load + SITL fleet |
| 4 | Latency from telemetry event to alert delivery | end-to-end timestamps |
| 6 | ZGC vs G1 under ingest load; p99 pause times | JFR + async-profiler |
| 6 | Allocation rate with and without the flyweight, under sustained ingest | GC logs |
| 6 | Recovery time and data loss when Kafka or the database is killed | chaos runs |
