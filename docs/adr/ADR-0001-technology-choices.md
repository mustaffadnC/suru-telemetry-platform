# ADR-0001 — Core technology choices

- **Status:** Accepted
- **Date:** 2026-07-29

## Context

SÜRÜ ingests high-volume telemetry from UAV and IoT fleets, processes and stores it, and sends commands back. The choices below were made under two constraints: (1) the system has to hold up under real load, and (2) the technologies should line up with what Java job postings in Turkey actually ask for.

## Decision

| Area | Choice |
|---|---|
| Language | Java 25 LTS |
| Framework | Spring Boot 4.1 (Spring Framework 7) |
| Build | Gradle 9.6.1, Kotlin DSL, version catalogue |
| Networking | Netty 4.2 |
| Messaging | Kafka in KRaft mode |
| Time series | TimescaleDB |
| Stream processing | Kafka Streams |
| Identity | Keycloak (OIDC) + Spring Security |
| Observability | OpenTelemetry + Grafana LGTM |

## Rationale

**Java 25 LTS, not 26.** Virtual threads and scoped values are final in 25. Choosing a non-LTS release in a portfolio project invites the objection "I would not run this in production". Java 26's structured concurrency (JEP 525) is attractive but still requires a preview flag, so it stays off the core path.

**Gradle, not Maven.** Maven is more common in the Turkish defence industry, so this is a deliberate deviation. The reason: multi-module layout, JMH integration and the configuration cache are all noticeably cleaner in Gradle. The counter-argument is accepted — Maven familiarity has to be demonstrated elsewhere.

**No `subprojects { }`.** Cross-project configuration is incompatible with the configuration cache and Isolated Projects. Shared settings live in convention plugins under `buildSrc`.

**Warnings are errors (`-Werror -Xlint:all`).** The counterpart of the `-Wall -Wextra -Wshadow -Wconversion` discipline in the ÇARGE firmware. The codebase does not accumulate warnings.

**TimescaleDB rather than QuestDB — for now.** QuestDB ingests 6–13× faster. But the platform's relational data (tenants, devices, rules, users, audit) already wants PostgreSQL, and keeping one engine preserves JOIN flexibility. **This decision is deliberately provisional: phase 3 measures both and the result is written up in ADR-0004.** The choice was not made without measurement — the measurement was deferred, and that is stated rather than hidden.

**Kafka in KRaft mode.** ZooKeeper-free deployment is the default now; there is no reason to operate two distributed systems.

## Consequences

- This is the first Java project in the portfolio, so phases 0–1 are deliberately plain Java with no Spring: the language and toolchain settle before a framework is added on top.
- The Gradle choice may need explaining when applying to Maven-heavy shops; this ADR is that explanation.
- The TimescaleDB decision is explicitly marked provisional. ADR-0004 will either confirm or overturn it.
