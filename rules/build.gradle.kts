plugins {
    id("suru.java-conventions")
}

description = "Rule evaluation and alert state machine — plain Java, no runtime dependencies."

dependencies {
    // NO RUNTIME DEPENDENCY IS EVER ADDED HERE, for the same reason as `protocol`.
    // The subtle part of alerting is *when* a condition becomes an alert, not how the
    // records arrive. Keeping this module free of Kafka means every debounce, hysteresis
    // and state-machine edge is exercised by an ordinary unit test with a fake clock,
    // instead of by standing up a broker and hoping the timing reproduces.
    // Enforced by ArchitectureTest — a failing test, not a comment.

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.platform.launcher)
}
