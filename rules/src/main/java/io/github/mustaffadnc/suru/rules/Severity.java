package io.github.mustaffadnc.suru.rules;

/** How urgently an alert wants attention. */
public enum Severity {
    /** Worth recording, not worth waking anyone. */
    INFO,
    /** Needs attention before it becomes a problem. */
    WARNING,
    /** The vehicle or the mission is at risk now. */
    CRITICAL
}
