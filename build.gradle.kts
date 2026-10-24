plugins {
    // Loaded here but not applied, so every module shares one copy of the Kotlin plugin. Without this line,
    // modules with different sets of plugins each load their own copy, which the Kotlin plugin does not support
    alias(libs.plugins.kotlin.jvm) apply false

    // Versioning and git tagging for the whole build: nebula tags the repository, not a single module
    alias(libs.plugins.nebula.release)

    // Same thing - nebula's release task lives in the root project, so the hook that publishes
    // to Maven Central as part of a release lives here too
    id("kolbasa.release")
}
