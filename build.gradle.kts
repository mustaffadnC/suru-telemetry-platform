// The root project is intentionally empty: shared configuration lives in the convention
// plugins under buildSrc, and modules apply it themselves. Plugins are declared here only
// to pin their versions in one place; they are applied by the modules that need them.

plugins {
    alias(libs.plugins.jmh) apply false
}

tasks.register("printModules") {
    val names = subprojects.map { it.name }.sorted()
    doLast { println(names.joinToString("\n")) }
}
