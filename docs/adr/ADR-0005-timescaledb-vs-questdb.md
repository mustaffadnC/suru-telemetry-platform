# ADR-0005 — TimescaleDB rather than QuestDB, now measured

- **Status:** Accepted
- **Date:** 2026-08-04
- **Supersedes the provisional part of:** [ADR-0001](ADR-0001-technology-choices.md)

## Context

ADR-0001 picked TimescaleDB and said so provisionally: QuestDB was known to ingest far faster, the
platform's relational data already wanted PostgreSQL, and the honest position was that the choice
had not been measured — only deferred, and stated as deferred.

This ADR is that measurement. Both engines were loaded with the same 100,000,000 rows, in the same
shape, on the same machine, **one after the other** — running them together would have measured the
disk contention rather than either engine.

Fairness was the first design problem. QuestDB is fed through its own line protocol rather than its
PostgreSQL wire compatibility, and its dimension columns are `SYMBOL`, its interned string type,
rather than `VARCHAR`. Comparing one vendor's fast path against another's slow one produces a number
that means nothing, and `VARCHAR` would have handed TimescaleDB an advantage QuestDB does not have
to concede.

## What was measured

| | TimescaleDB 2.29 | QuestDB 9.4.3 | Winner |
|---|---:|---:|---|
| Ingest | 102,778 rows/s | 1,599,460 rows/s | **QuestDB 15.6×** |
| Footprint | 0.84 GB | 3.99 GB | **TimescaleDB 4.8×** |
| Point query: one series, 1 hour | 1.0 ms p95 | 19.2 ms p95 | **TimescaleDB 19×** |
| Latest value per metric | 5.0 ms p95 | 60.8 ms p95 | **TimescaleDB 12×** |
| One metric across 50 devices | 3.2 ms p95 | 84.4 ms p95 | **TimescaleDB 26×** |

Full method, the non-comparable rows, and the ±30 % run-to-run spread these timings carry are in
[`docs/benchmarks.md`](../benchmarks.md). The TimescaleDB column is the run adjacent in time to the
QuestDB load; an earlier run was faster on every row, and using it would have flattered the
conclusion this ADR reaches.

**The published claim that prompted this was right, and irrelevant.** ADR-0001 cited "QuestDB
ingests 6–13× faster"; measured here it is 15.6×, so if anything the claim was conservative. The
mistake would have been to treat that as the deciding number.

## Decision

**Keep TimescaleDB.** The provisional choice in ADR-0001 is confirmed, on evidence that is not the
evidence originally assumed.

## Why the faster ingest does not decide it

The gateway sustains 1.44 M frames/s, and TimescaleDB absorbs 103 k rows/s. Those look
irreconcilable until the ratio between them is counted: **one MAVLink frame produces at most a
handful of stored measurements, and most frames produce none at all.** Only a curated set of
messages is decoded into metrics — heartbeats, parameter traffic and simulator ground truth are
published to Kafka and stored nowhere. Measured over the recorded flight, the pipeline writes well
under one row per frame.

More decisively: 102,778 rows/s is **roughly 100 devices each reporting 1,000 metrics every second**.
No fleet this platform is built for comes close. QuestDB's extra order of magnitude buys headroom
against a load that does not exist, and headroom that will never be used is not an advantage — it is
a column in a benchmark.

Kafka already absorbs the burst that would make ingest rate matter. The store is a consumer with a
durable queue in front of it; when it falls behind, the topic grows and the consumer catches up.
That is what the offset-after-write ordering exists for.

## Why footprint and query latency do decide it

**Storage is the recurring cost.** 0.84 GB against 3.99 GB is 4.8× less disk for the same data, and
telemetry accumulates forever while ingest rate stays flat. At retention measured in months, that
ratio compounds into the dominant operating cost of the whole platform.

**Query latency is what users experience.** A point query is 19× faster and a fleet-wide latest-value
lookup 12×. Those are the operations a dashboard performs constantly, and the ones an operator waits
on during an incident. Ingest rate is a number nobody watches; page load is a number everybody does.

**And after `V4` the query lead is total.** The one query QuestDB won — a 24-hour range in 5-minute
buckets, where it was 1.5× ahead — reversed to a 20× loss once the minute rollup carried a composite
index. That row is why this ADR was not written before the rollup investigation finished: the
comparison would have recorded a QuestDB advantage that was an artefact of a missing index on the
TimescaleDB side, and no reader could have known.

**And the platform is not only time series.** Tenants, devices, rules, users, audit — all of it is
relational, all of it needs joins and constraints and transactions against the telemetry. Keeping
one engine means one backup story, one migration tool, one connection pool, one set of operational
knowledge. Splitting across two would trade a real, permanent complexity cost for headroom that has
already been argued away.

## Consequences

- The decision is now settled rather than provisional, and ADR-0001's marker can be considered
  discharged.
- **The trigger to revisit is stated so it can be checked:** if sustained ingest approaches
  50,000 rows/s — roughly half of what was measured — the margin is gone and this comparison should
  be re-run rather than remembered. `suru.ingest.accepted` divided by rows written per record is the
  number to watch.
- QuestDB's advantage is real and was reproduced. Nothing here says it is the wrong tool; it says it
  is the wrong tool *for a workload dominated by storage cost and point queries rather than by
  ingest rate*.
- The comparison harness is checked in (`QuestDbComparisonIT`) and gated behind
  `-Dsuru.dbbench=true`, so re-running it is a command rather than a project.
