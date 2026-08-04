plugins {
    id("suru.java-conventions")
}

description = "Kafka Streams topology: device state, rule evaluation, alert publication."

dependencies {
    api(projects.rules)
    implementation(projects.protocol)
    implementation(projects.ingestGateway)

    implementation(libs.kafka.streams)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jsr310)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.kafka.streams.test.utils)
    testRuntimeOnly(libs.junit.platform.launcher)
}
