# SÜRÜ

[![CI](https://github.com/mustaffadnC/suru-telemetry-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mustaffadnC/suru-telemetry-platform/actions/workflows/ci.yml)

**A fleet telemetry and command-and-control platform for UAVs and IoT devices** — written in Java 25, built from the transport layer up.

> 🇹🇷 *Türkçe özet aşağıda.*

## Why this exists

Most telemetry backends on GitHub wrap a REST API around a database and call it a platform. SÜRÜ is built the other way round: it starts at the wire.

I wrote the firmware that produces this telemetry (an STM32 capsule, approved by TUSAŞ, first prize at IDEA4Defence) and the ground control station that consumes it (C++20/Qt6, MAVLink v2, validated against ArduPilot SITL). SÜRÜ is the missing third layer — the server side that ingests, processes, stores, alerts on, and commands back.

That means every design decision here is answerable from both ends of the link:

- **Two protocols, two different CRCs.** MAVLink v2 uses CRC-16/MCRF4XX; the ÇARGE capsule log frame uses CRC-16/CCITT-FALSE. Both use polynomial `0x1021` — and they are not interchangeable. The MAVLink one is routinely mistaken for CRC-16/X-25 because the reference C function is called `crc_accumulate` and the constant is `X25_INIT_CRC`; X-25 applies a final XOR of `0xFFFF` and MAVLink does not. Get it wrong and every packet reads as corrupt.
- **Real telemetry, not a fake generator.** Load comes from ArduPilot SITL instances flying real missions, and from recorded byte streams captured off an actual link.
- **Verified against an independent implementation.** The Java decoder is differential-tested against the reference Python decoder written for the firmware.

## Status — Phase 1 (protocol core) ✅

| Phase | Scope | Status |
|---|---|---|
| 0 | Build infrastructure, convention plugins, compose stack, CI, ADRs, CRC core | ✅ done |
| 1 | Protocol core: MAVLink v1/v2 + HK framing, resync, sequence-loss, differential tests, JMH | ✅ done |
| 2 | Ingest gateway: Netty, backpressure + load shedding, dedup, Kafka producer | 🚧 TCP + admission control + Kafka done; UDP, dedup and fleet load test remain |
| 3 | TimescaleDB schema, continuous aggregates, query API | ⬜ |
| 4 | Kafka Streams windowing, rules engine with debounce/hysteresis, alert state machine | ⬜ |
| 5 | Command path (outbox, ACK matching), Keycloak, multi-tenancy, audit log | ⬜ |
| 6 | OpenTelemetry, load tests, GC comparison, chaos tests, Helm/kind | ⬜ |
| 7 | Live map console, scripted demo, benchmarks write-up | ⬜ |

Nothing above is claimed as working until its phase is marked done and the numbers are in [`docs/benchmarks.md`](docs/benchmarks.md).

**Phase 0, verified:** `docker compose up -d` brings up all six services healthy, checked functionally rather than by container state: TimescaleDB 2.29.0 extension created and queried, a 3-partition Kafka topic created/described/deleted on a KRaft broker, Redis `PONG`, Keycloak/MinIO/Grafana all HTTP 200, OTLP port open. Idle footprint 2.2 GB.

**Phase 1, verified:** 34 tests green. The 36 KB recorded SITL stream decodes to **1058 frames with zero checksum errors and zero unknown messages**, after resyncing past 119 bytes of boot banner — reproducing the reference decoder's output exactly, under every input chunking from one byte upward. Decode throughput **568 MB/s** (16.6 M frames/s); table-driven CRC made the whole decoder 2.9–3.1× faster. Numbers and method in [`docs/benchmarks.md`](docs/benchmarks.md); protocol details and the reasoning behind resync in [`docs/protocol.md`](docs/protocol.md).

**Phase 2, so far:** the gateway takes MAVLink off a TCP socket, attributes it to a tenant and device, and publishes to Kafka keyed by device — 52 tests green, including two against a real broker in a container. Under load it applies the lossless remedy first: above 60 % pressure it stops reading and lets TCP's own flow control slow the sender, and only above that does it shed, bulk diagnostics first and heartbeats never. Reasoning in [ADR-0003](docs/adr/ADR-0003-backpressure-and-shedding.md).

> The shedding thresholds were wrong in the first draft — bulk was discarded at 50 % while reads paused at 80 %, throwing data away while a lossless option sat untried. Caught while writing the tests; a test now pins the invariant that a pressure band must exist where the gateway has stopped reading and is still losing nothing.

Three findings from phase 1 are written up rather than quietly fixed, because each one was a belief that measurement or testing overturned:

- **The convenience API lost data.** `endOfStream()` had a no-arg overload that discarded recovered frames. A frame held back by a corrupted length field surfaces only there — the overload lost one in its first use, ten minutes after being written, and was removed.
- **A false `HK\x01` header does not merely desynchronise, it parses.** The following frame's own magic supplies a plausible type and length, so a frame-shaped span is read and only the checksum rejects it. This is the concrete reason recovery advances one byte instead of skipping the claimed length.
- **The zero-copy design looked worthless until the CRC got faster.** Copying every payload cost 1–4 % while checksumming dominated, and 10–17 % once it did not. Judged on the first measurement alone, the flyweight would have been reasonable to delete.

## Architecture

```
  ArduPilot SITL x N        ÇARGE capsule logs         Generic IoT
  (MAVLink v2/UDP)          ('HK' framed binary)       (MQTT)
        |                          |                        |
        +--------------------------+------------------------+
                                   |
                    ┌──────────────▼───────────────┐
                    │   INGEST GATEWAY (Netty)     │
                    │  codec · auth · dedup ·      │
                    │  backpressure · load shed    │
                    └──────────────┬───────────────┘
                                   │ Kafka: telemetry.raw
              ┌────────────────────┼────────────────────┐
              │                    │                    │
    ┌─────────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
    │ STREAM PROCESSOR │  │  RULES ENGINE   │  │    ARCHIVER     │
    │ windowing,       │  │ thresholds,     │  │  Parquet→MinIO  │
    │ enrichment       │  │ debounce, FSM   │  │  cold tier      │
    └─────────┬────────┘  └────────┬────────┘  └─────────────────┘
              │                    │ Kafka: alerts
    ┌─────────▼────────┐           ▼
    │   TimescaleDB    │      [Notifier]
    └─────────┬────────┘
              │
    ┌─────────▼──────────────────────────────────────┐
    │  CONTROL PLANE (Spring Boot 4.1 + Modulith)    │
    │  REST + WebSocket + OpenAPI                    │
    └────────────────────────────────────────────────┘
```

Four deployables, each with a stated reason to exist — and, just as deliberately, a control plane that is *not* split into six microservices. The reasoning is in [ADR-0002](docs/adr/ADR-0002-service-boundaries.md).

## Build

Requirements: JDK 25, Docker Engine.

```bash
bash tools/bootstrap-gradle.sh
```

```bash
./gradlew check
```

```bash
docker compose -f deploy/docker-compose.yml up -d
```

## Design notes

- [ADR-0001](docs/adr/ADR-0001-technology-choices.md) — technology choices, including why Gradle over Maven and why the TimescaleDB decision is explicitly provisional until measured
- [ADR-0002](docs/adr/ADR-0002-service-boundaries.md) — service boundaries: why four deployables and not twelve

Architecture rules are enforced by tests, not documentation. `protocol` has no runtime dependencies and `ArchitectureTest` fails the build if that ever changes.

---

## 🇹🇷 Türkçe Özet

**SÜRÜ**, İHA ve IoT filolarından gelen telemetriyi yutan, işleyen, saklayan, alarm üreten ve komut geri gönderen bir platform. Java 25 ile, ulaşım katmanından itibaren yazılıyor.

Farkı şurada: bu telemetriyi **üreten** firmware'i (TUSAŞ onaylı, IDEA4Defence 1.'lik ödüllü STM32 kapsül) ve onu **tüketen** yer kontrol istasyonunu (C++20/Qt6, MAVLink v2) da ben yazdım. SÜRÜ zincirin eksik üçüncü halkası — sunucu tarafı.

Bu yüzden her tasarım kararı bağlantının iki ucundan da savunulabilir. Örnek: MAVLink v2 **CRC-16/MCRF4XX**, ÇARGE kapsül çerçevesi ise **CRC-16/CCITT-FALSE** kullanır. İkisi de `0x1021` polinomlu ama birbirinin yerine geçmez. MAVLink'inki, referans C fonksiyonu `crc_accumulate` ve sabiti `X25_INIT_CRC` adını taşıdığı için sürekli CRC-16/X-25 sanılır; X-25 son XOR olarak `0xFFFF` uygular, MAVLink uygulamaz. Yanlış varyantı seçen ayrıştırıcı *her* paketi bozuk görür.

Yük sahte veri üretecinden değil, gerçek görev uçan ArduPilot SITL örneklerinden ve gerçek bir hattan kaydedilmiş byte akışlarından geliyor. Java çözücü, firmware için yazılmış bağımsız Python referans çözücüye karşı differential test ediliyor.

Durum: **Faz 0 (iskele)** sürüyor. Yukarıdaki tablodaki hiçbir madde, fazı tamamlanıp ölçümleri `docs/benchmarks.md`'ye girmeden "çalışıyor" sayılmıyor.

## License

[MIT](LICENSE)
