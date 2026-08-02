plugins {
    id("suru.java-conventions")
}

description = "TimescaleDB schema, batch writer and query layer for telemetry."

dependencies {
    api(project(":protocol"))
    api(project(":ingest-gateway"))

    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    implementation(libs.flyway.core)
    runtimeOnly(libs.flyway.postgresql)
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.awaitility)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.flyway.postgresql)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}

// The recorded SITL stream is the only realistic source of metrics to load.
sourceSets.named("test") {
    resources.srcDir("../protocol/src/test/resources")
}
