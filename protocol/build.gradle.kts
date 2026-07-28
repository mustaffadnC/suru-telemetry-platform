plugins {
    id("suru.java-conventions")
}

description = "MAVLink v2 and ÇARGE 'HK' framing codec — plain Java, no runtime dependencies."

dependencies {
    // NO RUNTIME DEPENDENCY IS EVER ADDED HERE.
    // This module is deliberately dependency-free: testable without Netty, measurable on
    // its own under JMH, portable into another project. The rule is enforced by
    // ArchitectureTest — it is a failing test, not a comment.

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.platform.launcher)
}
