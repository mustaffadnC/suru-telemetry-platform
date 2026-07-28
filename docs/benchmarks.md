# Benchmarks and measurements

Every performance claim in this repository lands here with the numbers behind it, the hardware it
ran on, and the command that reproduces it. A claim without a number in this file is not a claim.

## Reference machine

| | |
|---|---|
| CPU | AMD Ryzen 5 7600X, 6 cores / 12 threads |
| Memory | 15 GiB available to the runtime |
| OS | Ubuntu 26.04 LTS on WSL2, kernel 6.18 |
| JDK | OpenJDK 25.0.3 (Temurin/Ubuntu build) |
| Storage | ext4 on NVMe (WSL virtual disk — *not* `/mnt/c`) |

Numbers from a laptop under WSL are not datacenter numbers. They are recorded to compare
alternatives against each other on identical hardware, not to advertise absolute throughput.

## Phase 0 — environment baseline

Measured 29 Jul 2026.

| Check | Result |
|---|---|
| `./gradlew check` | 11 tests green (8 CRC, 3 architecture) |
| Full infrastructure start (`docker compose up -d`, warm images) | all six services healthy in ~5 s |
| Idle memory footprint of the whole stack | 2.2 GiB |
| TimescaleDB extension | 2.29.0 |

## Planned measurements

These are the questions later phases have to answer with numbers, not adjectives.

| Phase | Question | Method |
|---|---|---|
| 1 | Bitwise vs table-driven CRC — how much does the lookup table actually buy? | JMH, throughput + `-prof gc` |
| 1 | Frame decode throughput in MB/s and packets/s; allocations per packet | JMH on recorded SITL stream |
| 2 | Sustained ingest rate before backpressure engages; behaviour when Kafka stalls | synthetic load + SITL fleet |
| 3 | Query p95 across time ranges; disk before/after compression | ≥100M rows loaded |
| 3 | TimescaleDB vs QuestDB ingest and query on identical data | feeds ADR-0004 |
| 4 | Latency from telemetry event to alert delivery | end-to-end timestamps |
| 6 | ZGC vs G1 under ingest load; p99 pause times | JFR + async-profiler |
| 6 | Recovery time and data loss when Kafka or the database is killed | chaos runs |
