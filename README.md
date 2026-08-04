# SÜRÜ

[![CI](https://github.com/mustaffadnC/suru-telemetry-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mustaffadnC/suru-telemetry-platform/actions/workflows/ci.yml)

**A fleet telemetry and command-and-control platform for UAVs and IoT devices** — written in Java 25, built from the transport layer up.

> 🇹🇷 *Türkçe özet aşağıda.*

> **Built with AI coding tools.** Much of the code here was scaffolded and iterated using Claude
> Code. The protocol research, the architectural decisions, and the benchmark methodology — what
> gets measured, against which reference, and what counts as done — are mine. Every number in this
> README comes from a run I set up and checked; the "beliefs overturned" section below is what that
> checking produced.

## Why this exists

Most telemetry backends on GitHub wrap a REST API around a database and call it a platform. SÜRÜ is built the other way round: it starts at the wire.

I wrote the firmware that produces this telemetry (an STM32 capsule, approved by TUSAŞ, first prize at IDEA4Defence) and the ground control station that consumes it (C++20/Qt6, MAVLink v2, validated against ArduPilot SITL). SÜRÜ is the missing third layer — the server side that ingests, processes, stores, alerts on, and commands back.

That means every design decision here is answerable from both ends of the link:

- **Two protocols, two different CRCs.** MAVLink v2 uses CRC-16/MCRF4XX; the ÇARGE capsule log frame uses CRC-16/CCITT-FALSE. Both use polynomial `0x1021` — and they are not interchangeable. The MAVLink one is routinely mistaken for CRC-16/X-25 because the reference C function is called `crc_accumulate` and the constant is `X25_INIT_CRC`; X-25 applies a final XOR of `0xFFFF` and MAVLink does not. Get it wrong and every packet reads as corrupt.
- **Real telemetry, not a fake generator.** Load comes from ArduPilot SITL instances flying real missions, and from recorded byte streams captured off an actual link.
- **Verified against an independent implementation.** The Java decoder is differential-tested against the reference Python decoder written for the firmware.

## Status — Phase 4 (rules engine and alerting) ✅

| Phase | Scope | Status |
|---|---|---|
| 0 | Build infrastructure, convention plugins, compose stack, CI, ADRs, CRC core | ✅ done |
| 1 | Protocol core: MAVLink v1/v2 + HK framing, resync, sequence-loss, differential tests, JMH | ✅ done |
| 2 | Ingest gateway: Netty, backpressure + load shedding, dedup, Kafka producer | ✅ done |
| 3 | TimescaleDB schema, continuous aggregates, query API, 100M-row measurement | ✅ done |
| 4 | Kafka Streams windowing, rules engine with debounce/hysteresis, alert state machine | ✅ done (webhook delivery; SMTP is an unwritten `AlertSink`) |
| 5 | Command path (outbox, ACK matching), Keycloak, multi-tenancy, audit log | ⬜ |
| 6 | OpenTelemetry, load tests, GC comparison, chaos tests, Helm/kind | ⬜ |
| 7 | Live map console, scripted demo, benchmarks write-up | ⬜ |

Nothing above is claimed as working until its phase is marked done and the numbers are in [`docs/benchmarks.md`](docs/benchmarks.md).

**Phase 0, verified:** `docker compose up -d` brings up all six services healthy, checked functionally rather than by container state: TimescaleDB 2.29.0 extension created and queried, a 3-partition Kafka topic created/described/deleted on a KRaft broker, Redis `PONG`, Keycloak/MinIO/Grafana all HTTP 200, OTLP port open. Idle footprint 2.2 GB.

**Phase 1, verified:** 34 tests green. The 36 KB recorded SITL stream decodes to **1058 frames with zero checksum errors and zero unknown messages**, after resyncing past 119 bytes of boot banner — reproducing the reference decoder's output exactly, under every input chunking from one byte upward. Decode throughput **568 MB/s** (16.6 M frames/s); table-driven CRC made the whole decoder 2.9–3.1× faster. Numbers and method in [`docs/benchmarks.md`](docs/benchmarks.md); protocol details and the reasoning behind resync in [`docs/protocol.md`](docs/protocol.md).

**Phase 2, verified:** the gateway takes MAVLink off TCP and UDP sockets and recovered ÇARGE capsule logs off a third port, attributes everything to a tenant and device, deduplicates, and publishes to Kafka keyed by device — 68 tests green, including two against a real broker in a container. Sustained ingest measured at **1.44 M frames/s (46.9 MB/s)** across 32 simultaneous connections, nothing shed and reads never paused.

Under load it applies the lossless remedy first: above 60 % pressure a TCP channel stops reading and lets TCP's own flow control slow the sender, and only above that does it shed — bulk diagnostics first, heartbeats never. UDP gets no such option and says so: with no back channel, declining to read just moves the loss into the kernel where it cannot be counted, so that transport keeps reading and sheds explicitly. Reasoning in [ADR-0003](docs/adr/ADR-0003-backpressure-and-shedding.md).

Two more corrections worth recording, since both were beliefs the work overturned:

> **The shedding thresholds were backwards in the first draft** — bulk discarded at 50 % while reads paused at 80 %, throwing data away while a lossless option sat untried. A test now pins the invariant that a pressure band must exist where the gateway has stopped reading and is still losing nothing.

> **The first load measurement was wrong by a factor of 850.** It reported 1,692 frames/s for a gateway wrapping a decoder already measured at 16.6 M frames/s — a gap far too large to be real. The cause was the test's own publisher collecting into a `CopyOnWriteArrayList`, whose `add` copies the whole array: the harness was the bottleneck, not the system. Details in [`docs/benchmarks.md`](docs/benchmarks.md).

**Phase 3, verified:** telemetry lands in TimescaleDB and comes back out through a query API. A narrow measurement table with columnar compression segmented by `(tenant, device, metric)`, hierarchical minute-and-hour rollups, and tiered retention — verified against a real TimescaleDB 2.29 rather than by trusting that the SQL ran, because a table can fail to be a hypertable while every statement succeeds. Bulk loading uses `COPY`, measured at **11.3× batched `INSERT`** (122,619 rows/s against 10,879). The consumer commits Kafka offsets *after* the database transaction, so a crash replays a batch rather than losing one; a test induces a write failure and asserts the records come back. The REST API picks its own resolution from the requested range, so a month-wide chart reads the hourly rollup and a live view reads raw.

**Measured at a hundred million rows:** ingest **102,778 rows/s** holding flat from the first batch to the last, **26.7× compression** (22.46 GB → 0.84 GB), and a point query at **1.0 ms p95**. Compression made reads *faster*, not slower — p95 roughly halved, because segmented rows are contiguous and the query was never CPU-bound. QuestDB was loaded with the same 100M rows for comparison and ingests **15.6× faster**; it also stores the data 4.8× larger and answers a point query 19× slower, which is why [ADR-0005](docs/adr/ADR-0005-timescaledb-vs-questdb.md) keeps TimescaleDB and states the ingest rate at which that should be revisited.

> **The minute rollup was slower than the raw data it summarises** — 70.9 ms p95 against 1.0 ms. The first explanation, real-time aggregation, was wrong: `materialized_only` defaults to *true* on 2.29, and a test asserting otherwise is what killed the hypothesis. The real cause was that **continuous aggregates do not inherit the raw table's indexes** — they are addressed as views but backed by their own hypertables, and TimescaleDB indexes them one column per `GROUP BY` rather than compositely. Migration `V4` adds the composite index: **70.9 ms → 2.3 ms**, and the gain grows with table size, which is the prediction that distinguished the right diagnosis from the wrong one.

> **A timestamp field was never wall clock.** All three ingest handlers were putting `System.nanoTime()` into a field named `receivedAtEpochNanos`. That counts from an arbitrary origin and is meaningful only as a difference — used as a timestamp it produces a number that looks like nanoseconds since 1970 and is not. Nothing had noticed because nothing had yet written it to a `TIMESTAMPTZ` column; rows would have landed around 1970 or 2262 depending on the machine's uptime.

**Phase 4, verified:** telemetry becomes alerts, and alerts reach an endpoint. A dependency-free rules module holds the semantics — threshold, geofence and telemetry-loss conditions, each with hysteresis, over a four-phase state machine that debounces both firing *and* recovery. The transition table is covered by construction: every test records the edge it traverses and the build fails if any row goes untested. A Kafka Streams topology runs it over changelog-backed state so a rebalance does not silently re-arm every pending alert. All three phase-4 scenarios — geofence breach, low battery, telemetry loss — pass end to end, and **an alert reaches a consumer 8.3 ms p50** after the telemetry that caused it.

> **96 % of the alert latency was a batching setting, not work.** The first measurement said 104.5 ms p50. The plain Kafka producer defaults `linger.ms` to 0; Kafka Streams overrides it to 100, and the p50 tracked that setting with a ~4 ms base. Alerts are rare by construction, so batching the alert topic buys nothing — the deployment sets 5 ms, which keeps changelog batching under load and still cuts idle alert latency twelve-fold. Numbers in [`docs/benchmarks.md`](docs/benchmarks.md).

> **Detecting silence needs the clock nobody recommends.** Telemetry loss is a condition on records that did not arrive, so it needs a timer, and Kafka Streams' `STREAM_TIME` — the deterministic, replayable, conventionally-correct choice — advances only with record timestamps. If *all* telemetry stops, stream time stops with it and the platform raises **zero** telemetry-loss alerts at exactly the moment every device has gone quiet. `WALL_CLOCK_TIME` catches that but fires on the whole fleet during any replay. The two are told apart by whether records are arriving *at all*, not by how far behind they are — suppressing on lag alone reintroduces the first bug, and a test is named after that. Reasoning in [ADR-0006](docs/adr/ADR-0006-detecting-silence.md).

> **The notifier gives up on purpose.** Delivery gates the offset commit, so retrying an undeliverable alert forever would block every alert behind it — losing one alert to protect the rest is the right trade, and it is made explicitly with a loud log line rather than by accident. Retries are bounded, backed off with full jitter (a burst of alerts sharing one cause would otherwise retry in lockstep against an already-struggling endpoint), and skipped entirely for permanent failures: retrying a 400 burns the budget a transient failure needed. Delivery runs as its own consumer, never on the stream thread, so a hung webhook cannot stop the platform noticing further incidents.

> **Trend rules are ordinary thresholds on a derived metric name.** A rolling window per device exposes `power.battery_remaining_pct#slope_per_min`, so "draining faster than 5 % a minute" is a `Threshold`, and it inherits hysteresis and debounce instead of reimplementing them. Which metrics get a window is read back out of the rules, so a trend rule cannot be deployed without one. This matters because a battery at 40 % is fine and a battery at 40 % falling 8 %/min has five minutes left — and no level threshold can tell them apart.

> **I claimed least squares was outlier-resistant, and it is not.** The write-up said a single outlier moves the fit by "roughly 1/n". That is true of the *mean*; a regression weights samples by their distance from the mean time, so an outlier near either end has high leverage. Measured on a steady −1 %/min discharge with one dropped reading: at four samples the fit lands on **−9.10 %/min** against the endpoint method's −10.00 — barely an improvement — and a mid-window dropout makes it report the battery **rising**. It only earns its keep once the outlier is outnumbered (−1.91 at eleven samples). The real defence is a minimum sample count before any slope is published at all, and there is a test named after the limitation so nobody removes that gate believing the estimator handles it.

> **A second measurement was wrong the way the first one in phase 2 was.** The initial latency run published 200 events at once and reported p95, p99 and max within 0.3 ms of each other — a distribution with no spread, which is a queue draining rather than a latency. It was measuring how long the burst took to empty, with the last event charged for the 199 ahead of it. Different tell from the 850× bug, same lesson: the harness was producing the number.

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

Durum: **Faz 0–3 tamamlandı** (protokol çekirdeği + ingest gateway + depolama ve sorgu). Faz 1'de 36 KB'lık kayıtlı SITL akışı **1058 çerçeveye, sıfır sağlama hatası ve sıfır bilinmeyen mesajla** çözülüyor; çözme hızı **568 MB/s**. Faz 2'de gateway 32 eşzamanlı bağlantı üzerinden **1.44 M çerçeve/s (46.9 MB/s)** sürdürülebilir ingest ölçtü, hiçbir şey düşürülmeden — ikisi konteynerdeki gerçek broker'a karşı olmak üzere testler yeşil.

Faz 3'te **100 milyon satır** gerçek bir TimescaleDB'ye yazıldı: **102.778 satır/s** ingest, **26.7× sıkıştırma** (22.46 GB → 0.84 GB) ve **1.0 ms p95** nokta sorgusu. Sıkıştırma okumayı yavaşlatmadı, **hızlandırdı** — sorgu hiçbir zaman CPU-bağımlı değildi, sıkıştırılmış satırlar bitişik olduğu için çok daha az sayfa okunuyor. Aynı 100M satır QuestDB'ye de yüklendi: QuestDB **15.6× hızlı yazıyor**, buna karşılık aynı veriyi 4.8× büyük saklıyor ve nokta sorgusunu 19× yavaş yanıtlıyor. Karar ve hangi ingest hızında yeniden gözden geçirilmesi gerektiği [ADR-0005](docs/adr/ADR-0005-timescaledb-vs-questdb.md)'te.

Yol boyunca çıkan en öğretici hata: **dakikalık rollup, özetlediği ham veriden yavaştı** (70.9 ms'e karşı 1.0 ms). İlk açıklamam yanlıştı; gerçek sebep, **continuous aggregate'lerin ham tablonun indekslerini miras almaması**. `V4` migration'ı kompozit indeksi ekliyor: **70.9 ms → 2.3 ms**.

Faz 4'te telemetri alarma dönüşüyor: eşik, coğrafi çit ve telemetri kaybı kuralları; hem tetiklemede hem de **düzelmede** debounce yapan dört fazlı durum makinesi (geçiş tablosunun tamamı testle kapatılıyor, kapatılmayan bir geçiş build'i kırıyor). Bir alarm, onu doğuran telemetriden **8.3 ms p50** sonra tüketiciye ulaşıyor.

İki ders: (1) Alarm gecikmesinin **%96'sı iş değil, bir batching ayarıydı** — Kafka Streams `linger.ms`'i 0 yerine 100'e çekiyor. (2) **En küçük kareler regresyonu aykırı değere dayanıklı değil**; "1/n kadar oynar" iddiam yanlıştı. Dört örnekte tek bir bozuk okuma eğimi −1'den −9.1'e taşıyor, ortadaysa **işaretini ters çeviriyor**. Çözüm tahmin ediciyi değiştirmek değil, yeterli örnek sayısı şartı koymak.

Yukarıdaki tablodaki hiçbir madde, fazı tamamlanıp ölçümleri `docs/benchmarks.md`'ye girmeden "çalışıyor" sayılmıyor.

## License

[MIT](LICENSE)
