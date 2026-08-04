# ADR-0006 — Detecting silence: wall-clock punctuation, guarded

- **Status:** Accepted
- **Date:** 2026-08-04

## Context

Three of the four alert conditions in phase 4 — threshold, geofence, rate — are functions of data
that arrived. A record shows up, the rule runs, an alert may follow. Telemetry loss is not like
them. It is a function of the records that **did not** arrive, and a processor that only runs when a
record arrives can never fire it: the event it is watching for is the absence of the event that
would wake it up.

This is the alert that matters most. A vehicle reporting a low battery is telling you something. A
vehicle telling you nothing may have landed, lost its link, or crashed, and the platform's job is to
say so.

So the rule needs a timer, and Kafka Streams offers two clocks to hang one on.

## The two clocks

**`STREAM_TIME`** advances with record timestamps. It is the conventional recommendation, and for
good reasons: processing becomes deterministic, a replay produces exactly the results the original
run did, and results do not depend on how fast the consumer happens to be.

It fails here, in the worst way available. If *all* telemetry stops — the gateway dies, a switch
fails, the network partitions — stream time stops with it. No records, no punctuation, no alerts.
**The platform would produce zero telemetry-loss alerts at precisely the moment every device in the
fleet had gone silent**, and the dashboard would look calm. The failure mode of the safe-looking
option is total silence during a total outage.

**`WALL_CLOCK_TIME`** always ticks, so it catches that case. Its hazard is replay. After a restart
with lag, or a deliberate reprocess, historical records are consumed at wall-clock speed: a backlog
spanning two hours is processed in minutes, and every device in it carries a timestamp two hours
old. Judged against the wall clock, every one of them has been silent for two hours. Unguarded,
**every restart would page the operator about the entire fleet** — which trains the operator to
ignore the alert, and an ignored alert is worse than an absent one because it costs the same to
maintain.

## The decision

**Wall-clock punctuation, with suppression while catching up.**

Missing a total outage is unacceptable in a way that a noisy restart is not: one is a silent failure
of the platform's core promise, the other is an annoyance with an obvious cause. So the clock that
cannot miss the outage is the one to build on, and the replay problem gets solved separately.

## Telling replay apart from an outage

The obvious signal is identical in both: record timestamps far behind the wall clock. Suppressing on
that signal alone would reintroduce the stream-time bug through the back door — a fleet that went
quiet six hours ago has enormous lag, and would be suppressed forever.

What separates them is **whether records are arriving at all**:

| | Records arriving | Timestamps | Correct response |
|---|---|---|---|
| Replay | yes, quickly | far behind | suppress — the devices are probably fine, their newer data has not been read yet |
| Outage | none | falling further behind every second | **evaluate** — this is what the rule is for |
| Normal | yes | current | evaluate |

So the guard is `lagging AND still consuming`, and never `lagging` alone. A test is named after that
distinction (`lagWithoutTrafficDoesNotSuppress`) because it is the exact mistake the design is
avoiding, and the version of this code that gets it wrong passes every other test in the suite.

The guard resolves itself without needing to know when a replay has finished: when the backlog runs
out, records stop arriving, the "still consuming" half goes false, and evaluation resumes. If the
newest record is still two hours old at that point, the device really has not been heard from and
the alert is correct.

## Consequences

- **One punctuation interval of delay** after the last record of a backlog, before staleness
  evaluation resumes. This is inherent, not incidental: nothing can distinguish "an old record
  arrived" from "a backlog is in progress" until it sees whether another one follows. One tick is
  the minimum possible.
- **A slow replay suppresses staleness detection for its whole duration.** If records trickle in
  with old timestamps for an hour, telemetry loss goes undetected for that hour. Accepted, because
  the alternative is alerting on the entire fleet during every catch-up, and because a trickling
  replay is itself a condition worth alerting on separately — that belongs to phase 6's
  observability work, where consumer lag is already a first-class metric.
- **Alerts are not deterministic under replay**, which is the price of leaving stream time behind.
  Reprocessing the same topic twice can produce different telemetry-loss alerts depending on how
  fast each run consumed. The other three conditions remain fully deterministic, since they depend
  only on record content and record timestamps.
- **The catch-up threshold is a tuning parameter with a real failure mode on each side.** Too low
  and normal jitter suppresses legitimate alerts; too high and a short replay still pages the fleet.
  It is set to one minute — comfortably longer than any staleness limit currently configured, and
  far shorter than a backlog worth suppressing for.
- Punctuation iterates every known device, so its cost is O(devices) per tick. At fleet sizes this
  platform targets that is nothing; at a hundred thousand devices per instance it would need a
  different structure, such as a priority queue keyed by next-deadline.
