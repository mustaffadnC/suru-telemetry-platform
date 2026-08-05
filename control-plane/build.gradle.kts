plugins {
    id("suru.java-conventions")
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

description = "Query and operator API over the telemetry store."

dependencies {
    implementation(project(":storage"))
    // For the Kafka header names the gateway writes and the MAVLink command codecs. The control
    // plane reads what the gateway produced; sharing the constants beats agreeing on strings.
    implementation(project(":ingest-gateway"))
    implementation(project(":protocol"))
    implementation(libs.kafka.clients)

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    // Identity arrives as a verified JWT rather than a header. The resource server validates the
    // signature, issuer and expiry before any of this application's code sees the request.
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation(libs.springdoc.webmvc)
    runtimeOnly(libs.postgresql)

    // The starter's own JUnit is excluded on the dependency rather than on the configuration.
    // Excluding at configuration level filters every resolved module regardless of who declared
    // it — including the JUnit 6 added just below, which then vanishes and takes the test
    // sources' imports with it.
    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.jupiter")
        exclude(group = "org.junit.platform")
    }
    // For MockMvcBuilders.apply(springSecurity()): the tests drive the real filter chain, so a
    // request without a valid token is rejected by the same code that rejects one in production.
    testImplementation("org.springframework.security:spring-security-test")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.postgresql)
}
