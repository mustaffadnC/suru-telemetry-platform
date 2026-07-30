# SÜRÜ — UAV/IoT Fleet Telemetry & Command-and-Control Platform

The Java/backend leg of Mustafa's defence-industry portfolio. Plan file:
`C:\Users\pc\.claude\plans\ben-java-dili-ile-peppy-spring.md`

## Portfolio chain (where the project's real value comes from)

On its own this repo is a telemetry platform; its actual strength is being the third link in a chain:

- **HavaKarakolu-Firmware** (ÇARGE, STM32) — the side that **produces** the telemetry. Log frame
  `'HK'|ver|type|len|payload|crc16`, CRC-16/CCITT-FALSE (`App/common/crc.c` → `hk_crc16_ccitt`),
  reference Python decoder `tools/hk_log_reader.py`.
- **kerkenez-gcs** (C++20/Qt6) — a ground control station speaking MAVLink v2. Recorded real SITL
  byte stream: `tests/data/sitl_stream.bin`.
- **SÜRÜ** (this repo) — the server side: ingests, processes, stores, alerts, commands back.

The interview sentence: *"I wrote the firmware that produces this telemetry and the platform that
consumes it."*

## Environment (set up 29 Jul 2026 — do not change)

- **Development happens inside WSL2 Ubuntu 26.04 (resolute)**, repo at `/home/pc/projects/suru`.
  From Windows: `\\wsl.localhost\Ubuntu\home\pc\projects\suru`.
- The repo is **deliberately not on `/mnt/c`** — the 9p filesystem slows Gradle and Docker I/O
  by 5–10×.
- **The Smart App Control problem does not apply here.** SAC only blocks unsigned Windows
  executables; Linux binaries inside WSL are unaffected. Unlike the firmware project, tests run
  locally.
- JDK 25 + Docker Engine (not Docker Desktop — systemd is already enabled) installed via apt.
- 12 vCPUs, 15 GB RAM, ~947 GB free on the WSL disk.

## Commands

```bash
# once, if the wrapper is missing
bash tools/bootstrap-gradle.sh

./gradlew check                                    # compile + test
docker compose -f deploy/docker-compose.yml up -d  # infrastructure
```

Quoting breaks when passing commands from PowerShell into WSL — write a script file and run it
with `wsl -d Ubuntu -e bash <script>`.

## Architecture rules

- **The `protocol` module is dependency-free.** Netty, Kafka and Spring may not leak into it — the
  rule is enforced by `ArchitectureTest`, not by a comment. The point is isolated testing,
  standalone JMH measurement, and portability.
- **`subprojects { }` is never used** — incompatible with the configuration cache and Isolated
  Projects. Shared settings live in `buildSrc/src/main/kotlin/suru.java-conventions.gradle.kts`.
- **Warnings are errors** (`-Werror -Xlint:all`), the counterpart of the firmware's
  `-Wall -Wextra -Wshadow -Wconversion`.
- Modules are added to `settings.gradle.kts` **when their phase starts**; no empty shell modules.
- Versions live only in `gradle/libs.versions.toml`; module files never spell out a bare version.

## Protocol module notes

- **Decoder API: `endOfStream(handler)` takes a handler and there is no no-arg overload.** A frame
  held back mid-stream — because a corrupted length field made the decoder wait for bytes that never
  came — is recovered only there. The convenience overload existed for about ten minutes and lost
  data in its first use.
- **Oracles live in `tools/`, goldens in `src/test/resources/`.** `hk-reference.py` and
  `mavlink-reference.py` regenerate the `.expected.txt` files; the Java tests compare against them.
  Both have a `--selftest`. Regenerate a golden only when the *format* changes deliberately — never
  to make a failing test pass.
- **`ArduPilotMegaDialect.java` is generated**, by `tools/generate-mavlink-dialect.py` from the
  MAVLink C headers vendored in kerkenez-gcs. Do not hand-edit.
- Full protocol reference and the reasoning behind resync: `docs/protocol.md`.

## Known traps

- **MAVLink is CRC-16/MCRF4XX, NOT X-25.** The names `crc_accumulate` / `X25_INIT_CRC` in the
  reference source mislead; X-25 applies a final XOR of `0xFFFF`, MAVLink does not. Wrong variant →
  every packet looks corrupt. ÇARGE uses CCITT-FALSE (non-reflected). Both use polynomial `0x1021`,
  both are different algorithms. `Crc16Test.mavlinkIsNotX25` locks this down.
- **ArchUnit must be ≥ 1.4.1 on Java 25.** Earlier releases bundle an ASM that cannot read class
  file major version 69: the importer silently skips *every* class and each rule fails with
  "failed to check any classes". That message reads like a rule violation but means the parser read
  nothing. Hit and fixed on the first build (1.4.0 → 1.4.2). The same trap will apply to any other
  bytecode-reading tool added later (JaCoCo, Error Prone, NullAway) — check ASM support first.
- Image tags are coarse in phase 0 (`latest`, major tags); **pinned to digests in phase 6**.
- `jqwik`'s JUnit Platform 6 compatibility is unverified — to be tested in phase 1; if incompatible,
  a property-based approach is built on JUnit 6's own facilities.

## Language convention

Code, comments, Javadoc, ADRs and commit messages in **English**. README is English with a Turkish
summary section (same convention as kerkenez-gcs).

## Phases

A phase is not done until its acceptance criterion is met (the firmware project's "a phase ends when
CI is green" discipline). Details in the plan file.

- **Phase 0** — scaffolding: Gradle multi-module, convention plugin, compose stack, CI,
  ADR-0001/0002, `Crc16` + tests ✅ *(verified 29 Jul 2026: 11/11 tests green; all six compose
  services healthy and functionally checked — TimescaleDB 2.29.0, Kafka KRaft topic lifecycle,
  Redis, Keycloak/MinIO/Grafana HTTP 200, OTLP 4317 open)*
- **Phase 1** — protocol core ✅ *(MAVLink v1/v2 + HK framing, resync, per-endpoint sequence loss,
  differential tests against two independent Python oracles, real 36 KB SITL recording decoding to
  1058 frames with 0 checksum errors, table-driven CRC + JMH)*
- **Phase 2** — ingest gateway: Netty, backpressure + load shedding, dedup, Kafka producer
- **Phase 3** — TimescaleDB schema + query API, QuestDB comparison → ADR-0004
- **Phase 4** — Kafka Streams windowing, rules engine (debounce/hysteresis), alert state machine
- **Phase 5** — command path (outbox, ACK matching), Keycloak, multi-tenancy, audit log
- **Phase 6** — OpenTelemetry, load tests, GC comparison, chaos tests, Helm/kind
- **Phase 7** — React console, demo script, README + GIF

## Rules

- **No Claude traces / Co-Authored-By in commits** (user preference, hard rule).
- Conventional Commits: `feat(protocol): ...`
- Every phase ends with its evidence in the README (measurement table / screenshot) — the repo stays
  presentable at any moment.
