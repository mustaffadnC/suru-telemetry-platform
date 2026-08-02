# ADR-0004 — Where deduplication belongs

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

Duplicate telemetry is real: a device retransmits, two network paths deliver the same datagram, a
link reconnects and replays its buffer. Downstream, duplicates inflate aggregates and corrupt
sequence-gap statistics.

The phase plan specified Redis with a TTL, keyed on `(deviceId, seq, timestamp)`. Building it
surfaced two problems with that specification, one of correctness and one of throughput.

## Problem 1: the key does not identify a duplicate

MAVLink's sequence number is **eight bits**, and it increments per message across an endpoint's
entire output — not per message type. At ArduPilot's typical combined rate of 40–50 msg/s it wraps
roughly **every five seconds**.

So `(deviceId, seq)` with any window longer than a few seconds does not identify duplicates. It
identifies wrap-around, and suppresses perfectly good telemetry. The key must include the message id
and a digest of the payload, and the window must stay well inside the wrap period.

Even that is not sufficient on its own. Low-entropy messages collide with themselves: two HEARTBEATs
seconds apart are frequently byte-identical, so a wrapped sequence would make one look like a
duplicate of the other — and a dropped heartbeat is exactly how the platform decides a healthy
vehicle has gone (ADR-0003).

**The asymmetry resolves it.** A duplicate heartbeat costs nothing, because liveness is idempotent:
seeing it twice says exactly what seeing it once said. A dropped heartbeat costs a false alarm. So
`CRITICAL` traffic bypasses deduplication entirely, and everything else — where a duplicate skews an
average and a rare false positive does not — is filtered. Deduplication is applied where duplicates
cost something and withheld where suppression would cost more.

## Problem 2: per-message Redis does not fit the throughput

The gateway sustains **1.44 M frames/s** (`docs/benchmarks.md`). A Redis round trip is on the order
of 50–100 µs even on loopback. Consulting Redis per message, synchronously, from the event loop
caps ingest at roughly **10–20 k/s per connection** — a regression of two orders of magnitude — and
blocks the event loop while doing it, which stalls every other connection that loop serves.

Pipelining or an async client removes the blocking but not the fundamental cost: one network round
trip per telemetry message, to answer a question that is almost always "no".

## Decision

**Deduplicate in-process, at the gateway, with a bounded time window.**
`InMemoryDuplicateFilter` holds recently seen identities keyed on
`(device, messageId, sequence, systemId, componentId, payload-digest)` for two seconds, in a map
capped by entry count. Both bounds are load-bearing: the window must stay inside the sequence wrap,
and the cap stops a sender that varies its payload from growing the map without limit. Overrunning
either costs a missed duplicate, never a suppressed distinct message.

**Do not put Redis on the per-message path.** Cross-instance deduplication is deferred, not
implemented here.

## Consequences

- Two gateway instances behind a load balancer each keep their own window, so a device that
  reconnects to a different instance is not deduplicated across them. This is a real gap and it is
  accepted knowingly.
- **When that gap needs closing, the right place is phase 4's stream processor, not the gateway.**
  Kafka Streams already keys by device and already maintains per-key state; deduplicating there
  costs a local state-store lookup rather than a network round trip, runs off the ingest hot path,
  and sees every instance's traffic because partitioning has already brought one device's messages
  together. The gateway is the worst place to solve a problem the topology solves for free one hop
  later.
- Deduplication is **opt-in** at construction rather than on by default. Suppression is only correct
  when duplicates are actually possible, and it is indistinguishable from deliberate replay —
  feeding the same recording twice down one connection produces byte-identical messages, which is
  also what a load test does.
- `DuplicateFilterStats` reports `exempt` separately from `passed`, so a high count of unfiltered
  heartbeats reads as normal rather than as the filter failing.
