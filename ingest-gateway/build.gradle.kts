plugins {
    id("suru.java-conventions")
}

description = "Netty telemetry ingest gateway: framing, admission control, publication to Kafka."

dependencies {
    api(project(":protocol"))

    implementation(platform(libs.netty.bom))
    implementation(libs.netty.transport)
    implementation(libs.netty.handler)
    implementation(libs.netty.buffer)

    implementation(platform(libs.micrometer.bom))
    implementation(libs.micrometer.core)

    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.awaitility)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.kafka)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}

// The recorded SITL stream lives with the protocol module's tests and is the only realistic
// input for an end-to-end ingest test. Reached by relative path rather than
// project(":protocol").file(...), which Isolated Projects does not allow — same approach as
// the benchmarks module.
sourceSets.named("test") {
    resources.srcDir("../protocol/src/test/resources")
}
