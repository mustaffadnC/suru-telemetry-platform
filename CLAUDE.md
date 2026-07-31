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
- **Any bytecode-reading tool must support class file major version 69 (Java 25).** ArchUnit 1.4.0
  bundled an ASM that could not: the importer silently skipped *every* class and each rule failed
  with "failed to check any classes" — which reads like a rule violation but means the parser read
  nothing. Fixed by 1.4.2. Check this first for JaCoCo, Error Prone, NullAway.
- **Optimisation payoffs are masked by whatever currently dominates.** Copying every payload cost
  1–4 % while the bitwise CRC dominated and 10–17 % after the table CRC replaced it, with nothing
  about the copying changed. Measure again after changing the profile; see
  [docs/benchmarks.md](docs/benchmarks.md).
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
- **Phase 2** — ingest gateway: Netty, backpressure + load shedding, dedup, Kafka producer
- **Phase 3** — TimescaleDB schema + query API, QuestDB comparison → ADR-0004
- **Phase 4** — Kafka Streams windowing, rules engine (debounce/hysteresis), alert state machine
- **Phase 5** — command path (outbox, ACK matching), Keycloak, multi-tenancy, audit log
- **Phase 6** — OpenTelemetry, load tests, GC comparison, chaos tests, Helm/kind
- **Phase 7** — React console, demo script, README + GIF

## Conventions

- Code, comments, Javadoc, ADRs and commit messages in **English**. README is English with a Turkish
  summary section (same convention as kerkenez-gcs).
- **No Claude traces / Co-Authored-By in commits** (user preference, hard rule).
- Conventional Commits: `feat(protocol): ...`
