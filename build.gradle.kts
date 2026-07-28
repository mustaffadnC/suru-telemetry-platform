// The root project is intentionally empty: shared configuration lives in the convention
// plugins under buildSrc, and modules apply it themselves.

tasks.register("printModules") {
    val names = subprojects.map { it.name }.sorted()
    doLast { println(names.joinToString("\n")) }
}
