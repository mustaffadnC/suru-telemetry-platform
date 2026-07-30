plugins {
    id("suru.java-conventions")
    alias(libs.plugins.jmh)
}

description = "JMH benchmarks for the protocol codecs."

dependencies {
    jmh(project(":protocol"))
}

// The recorded SITL stream lives with the protocol module's tests. Reaching it through a
// plain relative path rather than project(":protocol").file(...) keeps this build script
// free of cross-project configuration, which Isolated Projects does not allow.
sourceSets.named("jmh") {
    resources.srcDir("../protocol/src/test/resources")
}

jmh {
    // Two forks so JIT compilation luck in a single JVM cannot pass for a result.
    warmupIterations = 3
    iterations = 4
    fork = 2
    warmup = "1s"
    timeOnIteration = "1s"
    resultFormat = "TEXT"
    resultsFile = layout.buildDirectory.file("reports/jmh/results.txt")
}

// JMH writes its own harness classes, and generated code is not ours to keep warning-clean.
tasks.matching { it.name == "jmhCompileGeneratedClasses" }.configureEach {
    (this as JavaCompile).options.compilerArgs.removeAll(listOf("-Werror"))
}
