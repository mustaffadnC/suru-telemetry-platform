import org.gradle.api.tasks.testing.logging.TestExceptionFormat

// Shared configuration for every Java module.
// Note: cross-project configuration via `subprojects { }` is deliberately avoided —
// it is incompatible with the configuration cache and Isolated Projects. Convention
// plugins are the correct pattern.

plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    // Warnings are errors. This is the Java counterpart of the firmware project's
    // `-Wall -Wextra -Wshadow -Wconversion` discipline: the codebase never accumulates warnings.
    options.compilerArgs.addAll(
        listOf(
            "-Xlint:all,-processing",
            "-Werror",
        ),
    )
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = TestExceptionFormat.FULL
        showStandardStreams = false
    }
    // Run tests in parallel, scaled to the machine (12 vCPUs available).
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
}
