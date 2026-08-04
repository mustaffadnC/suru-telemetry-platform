pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "suru"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

dependencyResolutionManagement {
    // Modules may not declare their own repositories — dependency sources stay centralised.
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

// Modules are added when their phase starts; no empty shell modules are kept around.
//   Phase 1 → protocol
//   Phase 2 → ingest-gateway
//   Phase 3 → control-plane
//   Phase 4 → rules, stream-processor
//   Phase 6 → archiver
include("protocol")
include("ingest-gateway")
include("storage")
include("control-plane")
include("rules")
include("stream-processor")
include("benchmarks")
