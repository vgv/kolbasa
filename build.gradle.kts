plugins {
    // Versioning and git tagging for the whole build: nebula tags the repository, not a single module
    alias(libs.plugins.nebula.release)

    // Same thing - nebula's release task lives in the root project, so the hook that publishes
    // to Maven Central as part of a release lives here too
    id("kolbasa.release")
}
