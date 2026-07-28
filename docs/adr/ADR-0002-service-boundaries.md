# ADR-0002 — Service boundaries: why four deployables

- **Status:** Accepted
- **Date:** 2026-07-29

## Context

"Microservices" turns into decoration in a portfolio project very easily: twelve services, twelve databases, zero justification. The interesting question is not the service *count* but **why** each boundary is drawn where it is.

## Decision

Four deployables:

| Service | Why it is separate |
|---|---|
| **ingest-gateway** | Scales with connection count (one socket per device), network-bound CPU profile, latency budget in microseconds. No Spring MVC — Netty only. When the control plane grows tenfold this service must not, and when this service grows tenfold the control plane must not. |
| **control-plane** | Low request rate, logic-heavy, faces human operators. Spring Boot + Modulith. **Its internal modules are NOT separate services** — the boundaries are enforced with ArchUnit, demonstrating that splitting them is unnecessary. |
| **stream-processor** | Kafka Streams keeps state stores; it scales with partition count and its rebalance behaviour is completely unlike the others'. |
| **archiver** | Batch, latency-insensitive, low priority. Sharing a resource pool with the others would starve the hot path. |

## Rejected alternatives

**A single monolith.** Ingest has a fundamentally different scaling profile from the control plane. Having to scale the REST API just because 5,000 devices connected is wasteful — and worse, a GC pause in ingest would freeze the operator interface.

**One service per domain (devices, telemetry, rules, alerts, commands, tenancy → 6+ services).** These modules change at the same rate, read the same data and ship together. Splitting them creates a distributed-transaction problem and returns nothing for it. Keeping them in one process while enforcing the boundaries at compile time is the stronger engineering decision.

## Consequences

- Module boundaries inside `control-plane` must be protected by tests; without an ArchUnit rule the monolith degrades into mud.
- The contract between the four services is the set of Kafka topic schemas; schema changes have to stay backward compatible.
- This ADR is the answer to "why didn't you build twelve services" — declining to draw a boundary is also a decision, and it gets justified.
