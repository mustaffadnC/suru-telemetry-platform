# ADR-0003 — Backpressure and load shedding at the ingest gateway

- **Status:** Accepted
- **Date:** 2026-07-31

## Context

The gateway's input rate is set by how many aircraft are flying and what they have to say. Its
output rate is set by Kafka, the network, and whatever else is happening in the cluster. These two
numbers are unrelated, so there will be moments when input exceeds output.

The question is not whether to lose messages. It is whether the loss is chosen and counted, or
accidental and invisible.

Unbounded buffering is the tempting non-answer. It does not prevent loss; it converts loss into
latency, then into an out-of-memory kill — at which point everything is lost rather than the least
important thing, and it happens at the worst possible moment, during the incident that caused the
surge.

## Decision

Two mechanisms, applied in a fixed order, with the lossless one always tried first.

### 1. Pause reading (lossless) — the first remedy

Above 60 % pressure a TCP channel sets `autoRead(false)`. The socket receive buffer fills, the TCP
window closes, and the sender is slowed by TCP's own flow control. **No message is discarded and
nobody has to decide which one to sacrifice.**

Reading resumes below 30 %. The two thresholds differ on purpose: a single threshold would flip the
read gate on every message once pressure settled near it, costing a syscall each way for nothing.

### 2. Shed by priority (lossy) — only above the pause point

Every shedding threshold sits *above* the pause watermark:

| Band | Shed above | Examples |
|---|---|---|
| CRITICAL | never | HEARTBEAT, SYS_STATUS, COMMAND_ACK, STATUSTEXT |
| HIGH | 95 % | GLOBAL_POSITION_INT, ATTITUDE, GPS_RAW_INT, BATTERY_STATUS |
| NORMAL | 85 % | everything else, including messages this build does not recognise |
| BULK | 75 % | SIMSTATE, AHRS, MEMINFO, VIBRATION, RC_CHANNELS, SERVO_OUTPUT_RAW |

So shedding begins only in the band where pausing has already been applied and pressure kept
climbing. In practice that means one of two things: the backlog was already in flight before the
pause took effect, or the traffic is UDP and no pause was ever available.

**An earlier revision of this policy had the ordering backwards** — bulk was shed at 50 % while reads
paused at 80 %, so the gateway threw data away while a lossless option sat untried. The mistake was
caught while writing the tests, and `AdmissionControllerTest.pausingPrecedesShedding` now pins the
invariant: there must exist a pressure band in which the gateway has stopped reading and is still
discarding nothing.

### Why HEARTBEAT is never shed

Absence of a heartbeat is precisely how the platform concludes a vehicle is gone. Shedding
heartbeats under load would manufacture "telemetry lost" alarms for aircraft that are flying
perfectly well — and it would do so during the incident that caused the load, when operators can
least afford to be lied to. Losing a position sample degrades the picture; losing a heartbeat
corrupts it.

Unrecognised message ids are classified NORMAL rather than BULK for a related reason: quietly making
the unfamiliar the most disposable thing would hide a dialect gap instead of surfacing it.

## Why the two transports cannot share a strategy

This is the part that determines the whole design.

**TCP has a back channel.** Refusing to read propagates all the way to the sender. Backpressure is
real, and the gateway can decline work without losing any.

**UDP has none.** Declining to read a datagram socket does not slow anyone down — the kernel simply
discards datagrams once the receive buffer is full, silently, and the count is not reliably visible
to the application. A gateway that "applies backpressure" to UDP by not reading has not applied
backpressure at all; it has arranged for invisible loss and told itself it was being careful.

So UDP ingest must keep reading and shed explicitly here, trading silent loss for measured loss.
That is strictly worse than what TCP gets, and it is the honest option: an operator can act on a
shed counter and cannot act on datagrams that vanished in the kernel.

## Consequences

- Pressure is measured as **in-flight publications**, not queue length. A publication is in flight
  until the broker acknowledges it, so pressure rises the moment the downstream slows — before any
  queue has grown enough to matter and long before memory is a concern. A publisher completing its
  future on hand-off rather than on acknowledgement would report no pressure however badly Kafka was
  struggling, which is why `TelemetryPublisher` specifies the stronger contract.
- The read gate is re-evaluated between read batches, so a single large batch can overshoot the
  pause threshold before it takes effect. Capacity must therefore exceed the frames in one socket
  read, or the gateway will shed in situations where pausing alone should have sufficed. Default
  capacity is 8192; a 64 KB read of the densest observed traffic is on the order of a few hundred
  frames.
- `shedCritical` must always be zero. It is reported separately from the other bands precisely so
  that a non-zero value reads as a bug rather than as congestion.
- `readPauses` is a leading indicator: a rising pause rate with zero shedding means the gateway is
  coping only because it keeps telling senders to wait. That is the warning that arrives *before*
  any data is lost, and it is the number to alert on.
