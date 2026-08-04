# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**SÜRÜ** — a UAV/IoT fleet telemetry and command-and-control platform in Java 25, built from the
transport layer up. The Java/backend leg of a defence-industry portfolio. Plan file:
`C:\Users\pc\.claude\plans\ben-java-dili-ile-peppy-spring.md`.

Its value comes from being the third link in a chain the same author wrote end to end:

- **HavaKarakolu-Firmware** (ÇARGE, STM32) **produces** the telemetry. Log frame
  `'HK'|ver|type|len|payload|crc16`, CRC-16/CCITT-FALSE (`App/common/crc.c` → `hk_crc16_ccitt`),
  reference Python decoder `tools/hk_log_reader.py`.
- **kerkenez-gcs** (C++20/Qt6) speaks MAVLink v2 and holds the recorded real SITL byte stream
  (`tests/data/sitl_stream.bin`), copied into this repo's test resources.
- **SÜRÜ** (here) is the server side: ingests, processes, stores, alerts, commands back.

## Environment (set up 29 Jul 2026 — do not change)

- **Development happens inside WSL2 Ubuntu 26.04 (resolute)**, repo at `/home/pc/projects/suru`.
  From Windows: `\\wsl.localhost\Ubuntu\home\pc\projects\suru`.
- The repo is **deliberately not on `/mnt/c`** — the 9p filesystem slows Gradle and Docker I/O
  by 5–10×.
- JDK 25 + Docker Engine (not Docker Desktop — systemd is already enabled) installed via apt.
- **Smart App Control does not apply here.** SAC only blocks unsigned Windows executables; Linux
  binaries inside WSL are unaffected, so unlike the firmware project, tests run locally.
- 12 vCPUs, 15 GB RAM.

Two WSL-specific traps, both hit already:

- **Quoting breaks when passing commands from PowerShell into WSL.** `$(...)`, `||` and `\$` get
  eaten by the PowerShell parser. Write a script file and run `wsl -d Ubuntu -e bash <script>`.
- **Do not write long-running output to `/tmp`.** The distro shuts down once its last process exits
  and systemd clears `/tmp` on the next boot — a background benchmark's log vanished between
  finishing and being read. Write under `build/`, or capture stdout.

## Commands

```bash
bash tools/bootstrap-gradle.sh   # once, only if gradlew is missing
```

```bash
./gradlew check
```

```bash
docker compose -f deploy/docker-compose.yml up -d
```

```bash
docker compose -f deploy/docker-compose.yml down
```

**Running one test class or method.** Gradle caches test results aggressively, so a re-run with no
source change reports `UP-TO-DATE` and silently executes nothing — `--rerun` is usually what you
want:

```bash
./gradlew :protocol:test --rerun --tests '*HkDecoderTest*'
```

```bash
./gradlew :protocol:test --rerun --tests '*MavlinkDecoderTest.resyncsPastBootBanner'
```

**Benchmarks.** The full suite takes ~6 minutes; run it in the background:

```bash
./gradlew :benchmarks:jmh
```

For one benchmark class, build the uber-jar once and invoke JMH directly — much faster to iterate,
and it takes standard JMH flags:

```bash
./gradlew :benchmarks:jmhJar && java -jar benchmarks/build/libs/benchmarks-jmh.jar 'MavlinkDecodeBenchmark' -f 2 -wi 3 -i 4 -w 1s -r 1s
```

**Oracles and generated files.** All three scripts have a `--selftest` that checks them against
values fixed by the spec; run that before trusting a regeneration:

```bash
python3 tools/hk-reference.py --selftest && python3 tools/mavlink-reference.py --selftest && python3 tools/generate-mavlink-dialect.py --selftest
```

```bash
python3 tools/hk-reference.py --generate protocol/src/test/resources/hk
```

```bash
python3 tools/mavlink-reference.py --header <path-to>/ardupilotmega.h --generate protocol/src/test/resources/mavlink protocol/src/test/resources/mavlink/sitl_stream.bin
```

```bash
python3 tools/generate-mavlink-dialect.py <path-to>/ardupilotmega.h -o protocol/src/main/java/io/github/mustaffadnc/suru/protocol/mavlink/ArduPilotMegaDialect.java
```

**There is no separate lint task.** Style is enforced by the compiler (`-Werror -Xlint:all`, set in
the convention plugin) and by `ArchitectureTest`; both run under `check`.

## Architecture

Four deployables are planned, each with a stated reason to exist — and, just as deliberately, a
control plane that is *not* split into six microservices. Reasoning in
[ADR-0002](docs/adr/ADR-0002-service-boundaries.md). Only `protocol` and `benchmarks` exist so far;
modules are added to `settings.gradle.kts` **when their phase starts**, never as empty shells.

```
device → ingest-gateway (Netty) → Kafka → stream-processor / rules → TimescaleDB
                                                     ↓
                                          control-plane (Spring Modulith)
```

### The decoder pattern (the part worth reading several files to understand)

`MavlinkDecoder` and `HkDecoder` are the same machine over different framings, and every design
choice in them exists to survive a link that is not clean:

- **Buffering.** A growable `byte[]` with `readPos`/`writePos`. `feed()` appends, parses everything
  it can, then compacts. Because the parser always advances at least one byte, leftover is bounded
  by one maximum frame — the buffer cannot grow without bound on garbage input.
- **`parse(handler, atEnd)`.** The `atEnd` flag is the entire difference between two readings of the
  same bytes: mid-stream a frame that runs past the buffer is *incomplete*, so the decoder waits; at
  end of stream it is *garbage*, so it resyncs past. This is also what makes the Java decoders agree
  byte for byte with the Python oracles, which are whole-blob parsers.
- **Recovery is always `readPos++`.** Never skip by the length the failed frame claimed — a
  corrupted length field would otherwise step over every real frame behind it. Pinned by
  `MavlinkDecoderTest.survivesCorruptedLengthField`.
- **Flyweight frames.** One reusable `MavlinkFrame` / `HkRecord` is handed to the callback and is
  valid only for its duration; callers use `copyPayload()` or `toImmutable()` to keep data. Decoding
  allocates nothing per frame.
- **Counters are split by diagnosis**, not lumped: `checksumErrors` (real corruption) vs
  `unknownMessages` (well-formed, unverifiable) vs `resyncBytes` (misalignment). Conflating them is
  what makes a healthy link on the wrong dialect look broken.

### Why MAVLink needs a dialect

A MAVLink checksum absorbs one byte beyond the frame: the message's `CRC_EXTRA`, derived from its
field definitions, so peers that disagree about a layout fail loudly instead of misreading fields.
The consequence is that **a frame cannot be validated without knowing the message** — hence
`MavlinkDialect`, and hence unknown messages being counted separately rather than guessed at.

Full protocol reference and the reasoning behind resync: [docs/protocol.md](docs/protocol.md).

### Test strategy

Both decoders are differential-tested against **independent Python implementations** in `tools/`,
not against themselves. Each side renders a canonical text dump; the Java test asserts equality with
the committed `.expected.txt` golden, under every input chunking from one byte upward.

Floats in that dump are raw IEEE-754 hex on both sides **on purpose**: printing decimals would
compare Java's half-up rounding against Python's half-even instead of comparing what was read from
which offset.

Regenerate a golden only when the *format* changes deliberately — **never to make a failing test
pass.**

### Ingest gateway (phase 2)

- **Pausing precedes shedding, always.** Every shed threshold sits above the read-pause watermark
  because pausing a TCP read loses nothing and shedding always loses something. Pinned by
  `AdmissionControllerTest.pausingPrecedesShedding`. Getting this backwards is easy — the first
  draft did — and the symptom is data discarded while a lossless remedy was untried.
- **`shedCritical` must always be zero.** It is a separate counter so a non-zero value reads as a
  bug, not as congestion.
- **Pressure is in-flight publications, not queue depth.** `TelemetryPublisher`'s future must
  complete on broker acknowledgement, not on hand-off — a publisher that completes early reports no
  pressure however badly Kafka is struggling.
- **Kafka records are keyed by `tenant/device`** so one device stays on one partition. Downstream
  sequence-gap detection assumes it; round-robin partitioning would look exactly like packet loss.
- Payload goes to Kafka **unwrapped**, metadata in headers — no schema at this layer to drift from
  the protocol module.
- The read gate is re-checked between read batches, so capacity must exceed frames-per-batch or the
  gateway sheds where pausing should have sufficed.
- **UDP never touches `autoRead`.** Pausing a datagram socket does not slow the sender; it moves the
  loss into the kernel where it cannot be counted. That transport keeps reading and sheds.
- **A wait condition that is already true when the wait begins is not a wait.** The Kafka IT first
  waited for `inFlight == 0`, which holds before the gateway has read a byte — it returned instantly
  and raced the ingest. Wait for the *accepted* count first, then for drain, then `flush()`.
- **Measurement harnesses sit on the hot path too.** The load test's publisher used a
  `CopyOnWriteArrayList` and became the bottleneck, reporting 1,692 frames/s against a real
  1.44 M frames/s. When a number disagrees with a component measurement by orders of magnitude,
  suspect the harness first.
- Logback defaults to DEBUG with no config, putting Netty's startup probe and per-frame logging on
  the ingest path. `logback.xml` (main) and `logback-test.xml` (test) pin the levels.
- **Deduplication never touches CRITICAL traffic** and is **opt-in**. MAVLink's sequence is 8-bit
  and wraps every ~5 s, so the key must include a payload digest and the window must stay inside the
  wrap; even then, near-identical heartbeats would collide. Dedup runs *after* admission and must
  `release()` on suppression, or pressure leaks and the gateway wedges shut. Redis was rejected for
  the per-message path — ADR-0004.
- **A separate port per protocol, not sniffing.** MAVLink and HK are distinguishable by magic
  bytes, but a MAVLink link routinely opens with a text boot banner, so first-frame guessing
  misclassifies it. HK uploads have no system id (device = link) and rely on `endOfStream` to tell a
  torn tail from a frame in transit.

### Storage and query (phase 3)

- **A continuous aggregate does not inherit the raw table's indexes.** It is addressed as a view but
  backed by its own hypertable. TimescaleDB auto-indexes one column per `GROUP BY`
  — `(tenant_id, bucket)`, `(device_id, bucket)`, `(metric, bucket)` — so a query filtering all
  three uses one and filters the rest, and its cost tracks total rollup size rather than the series
  asked for. `V4__rollup_indexes.sql` adds the composite; measured 4.8× at 10M rows, and at 100M it
  had made the minute rollup *slower than scanning raw*.
- **`materialized_only` defaults to TRUE on 2.29.** Real-time aggregation is not in play unless
  switched on. A widely-documented explanation that does not apply to the version in hand.
- **`add_columnstore_policy` is a procedure** (`CALL`, not `SELECT`); retention and cagg policies are
  functions. The columnstore policy is still recorded as `proc_name = 'policy_compression'`.
- Continuous aggregates and policy functions **cannot run inside a transaction** — those migrations
  carry a sibling `.conf` with `executeInTransaction=false`.
- **Compression made reads faster, not slower** (p95 1.6 ms → 0.7 ms): segmented rows are contiguous
  and 26× smaller, so far fewer pages are touched. Do not assume decompression is a read tax here.
- Offsets are committed **after** the database write, so a crash replays rather than loses. The
  writer tolerates that because a replayed row is byte-identical; only `samples` moves.
- **Device ids contain `/`** (`link/sysN`), so they are query parameters in the API, never path
  segments — Spring rejects an encoded slash.
- Benchmarks are gated behind `-Dsuru.dbbench=true` and **must run one engine at a time**; two
  databases on one disk measure contention.
- **QuestDB reserves the table name `telemetry`** for its own internal telemetry. Creating one is
  silently accepted, `tables()` then lists nothing, and the line protocol logs "could not get table
  writer" per row while a handful leak through — 9 rows of 1000, and three million error lines. The
  comparison uses `suru_telemetry`.

## Invariants

- **`protocol` has no runtime dependencies.** Netty, Kafka and Spring may not leak in; the rule is
  enforced by `ArchitectureTest`, not by a comment. The point is isolated testing, standalone JMH
  measurement, and portability.
- **`endOfStream(handler)` takes a mandatory handler.** A frame held back mid-stream — because a
  corrupted length field made the decoder wait for bytes that never came — is recovered only there.
  A no-arg convenience overload existed for about ten minutes and lost data in its first use.
- **`ArduPilotMegaDialect.java` is generated.** Do not hand-edit; regenerate (command above). The
  generator's output is byte-identical to the committed file, which is worth re-checking after any
  header upgrade.
- **`subprojects { }` is never used** — incompatible with the configuration cache and Isolated
  Projects. Shared settings live in `buildSrc/src/main/kotlin/suru.java-conventions.gradle.kts`.
- Versions live only in `gradle/libs.versions.toml`; module files never spell out a bare version.
- **Warnings are errors** everywhere except JMH's generated harness, which is exempted explicitly in
  `benchmarks/build.gradle.kts`.

## Known traps

- **MAVLink is CRC-16/MCRF4XX, NOT X-25.** The names `crc_accumulate` / `X25_INIT_CRC` in the
  reference source mislead; X-25 applies a final XOR of `0xFFFF`, MAVLink does not. Wrong variant →
  every packet looks corrupt. ÇARGE uses CCITT-FALSE (non-reflected). Both use polynomial `0x1021`;
  they are different algorithms. `Crc16Test.mavlinkIsNotX25` locks this down.
- **A false `HK\x01` header does not merely desynchronise — it parses.** The following frame's own
  magic supplies a plausible type and length, so a frame-shaped span is read and only the checksum
  rejects it. Concrete reason recovery advances one byte.
- **MAVLink v2 truncation stops at one byte, never zero.** "Trailing zeros are truncated" reads as
  "remove them all"; it is not. In the 1058-frame SITL recording **no** frame has `len=0` and eight
  carry a single zero byte — a VFR_HUD whose payload truncates to nothing is still sent as `len=1`.
  A round trip through this repo's own decoder cannot catch this, because it reconstructs the zeros
  either way. Only the real bytes disagree.
- **Any bytecode-reading tool must support class file major version 69 (Java 25).** ArchUnit 1.4.0
  bundled an ASM that could not: the importer silently skipped *every* class and each rule failed
  with "failed to check any classes" — which reads like a rule violation but means the parser read
  nothing. Fixed by 1.4.2. Check this first for JaCoCo, Error Prone, NullAway.
- **Optimisation payoffs are masked by whatever currently dominates.** Copying every payload cost
  1–4 % while the bitwise CRC dominated and 10–17 % after the table CRC replaced it, with nothing
  about the copying changed. Measure again after changing the profile; see
  [docs/benchmarks.md](docs/benchmarks.md).
- **Continuous aggregates do not inherit the raw hypertable's indexes.** They are addressed as
  views, which makes it look like they do, but each is backed by its own hypertable and TimescaleDB
  indexes them one index per `GROUP BY` column, never compositely. A rollup was slower than the raw
  data it summarised (70.9 ms vs 1.0 ms) until `V4` added `(tenant_id, device_id, metric, bucket
  DESC)`. Note `materialized_only` defaults to **true** on 2.29 — real-time aggregation is *not*
  the usual explanation here, and assuming it was cost an hour.
- **A QuestDB table named `telemetry` collides with the server's own internal one.** The `CREATE` is
  accepted, `tables()` comes back empty, rows vanish, and the log fills with "could not get table
  writer". Use a prefixed name.
- **Kafka Streams overrides `linger.ms` to 100** where the plain producer defaults to 0. On a
  latency-sensitive topic that setting *is* the latency: alert delivery was 104.5 ms p50 until it
  was lowered, 8.3 ms at 5 ms, 4.6 ms at 0.
- **`TopologyTestDriver.advanceWallClockTime` fires each wall-clock punctuator once per call**,
  however far it moves the clock — advancing 20 s against a 1 s schedule is one tick, not twenty.
  Verified with a probe rather than assumed. Tests that jump exercise a cadence production never
  has and hide anything depending on the previous tick.
- **Least-squares regression is not outlier-robust, and "moves by 1/n" is wrong.** That intuition
  holds for the *mean*; a regression weights samples by distance from the mean time, so an outlier
  near either end has high leverage. On a steady −1 %/min series with one dropout: n=4 outlier at
  the end → −9.10 (endpoint method −10.00, so barely better); n=4 in the middle → **+1.90**, the
  wrong sign; n=11 at the end → −1.91. The defence is a minimum sample count, not the estimator.
- **A latency distribution with no spread is not a latency distribution.** p95 ≈ p99 ≈ max means the
  samples are not independent — usually a burst published at once and then drained, where the last
  sample is charged for everything ahead of it. Companion tell to the phase-2 850× bug: both times
  the harness produced the number.
- Compose image tags are coarse (`latest`, major tags); **pinned to digests in phase 6**.

## Phases

A phase is not done until its acceptance criterion is met, with evidence in the README (measurement
table / screenshot) — the repo stays presentable at any moment.

- **Phase 0 ✅** — scaffolding: Gradle multi-module, convention plugin, compose stack, CI,
  ADR-0001/0002, `Crc16`. Six compose services verified functionally, not by container state.
- **Phase 1 ✅** — protocol core: MAVLink v1/v2 + HK framing, resync, per-endpoint sequence loss,
  differential tests against two Python oracles, table-driven CRC + JMH. The real 36 KB SITL
  recording decodes to 1058 frames, 0 checksum errors, 0 unknown messages, 119 bytes of boot banner
  resynced; 568 MB/s.
- **Phase 2 ✅** — ingest gateway: Netty TCP + UDP + a third port for ÇARGE capsule log uploads,
  admission control (read-pause then priority shedding), tenant/device attribution, in-process
  deduplication, Kafka publisher (idempotent, device-keyed), Micrometer binding, ADR-0003/0004,
  Testcontainers integration tests. Measured 1.44 M frames/s across 32 connections.
- **Phase 3 ✅** — storage and query: TimescaleDB schema (hypertable, columnstore, hierarchical
  rollups, tiered retention), `COPY` bulk writer (11.3× batched INSERT), Kafka consumer with
  offset-after-write, Spring Boot query API choosing its own resolution, 100M-row measurement, and
  the QuestDB comparison → ADR-0005.
- **Phase 4 ✅** — rules engine and alerting: dependency-free `rules` module (threshold, geofence,
  staleness; hysteresis in the condition, debounce in a four-phase state machine with enforced
  transition coverage), Kafka Streams topology over changelog-backed stores, rolling windows
  exposing min/max/mean/stddev/slope as derived metrics so trend rules are ordinary thresholds,
  webhook notifier with bounded retries and jittered backoff. All three scenarios end to end;
  alert delivery measured at 8.3 ms p50 → ADR-0006. **SMTP delivery is not written** — it is a
  second `AlertSink` implementation and nothing else depends on it.
- **Phase 5** — command path (outbox, ACK matching), Keycloak, multi-tenancy, audit log
- **Phase 6** — OpenTelemetry, load tests, GC comparison, chaos tests, Helm/kind
- **Phase 7** — React console, demo script, README + GIF

## Conventions

- Code, comments, Javadoc, ADRs and commit messages in **English**. README is English with a Turkish
  summary section (same convention as kerkenez-gcs).
- **No Claude traces / Co-Authored-By in commits** (user preference, hard rule).
- Conventional Commits: `feat(protocol): ...`
