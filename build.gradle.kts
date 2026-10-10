plugins {
    // Versioning and git tagging for the whole build: nebula tags the repository, not a single module
    alias(libs.plugins.nebula.release)

    // Same thing - staging and releasing to Maven Central is a root-only concern, not a module concern.
    // That is how the nexus-publish plugin inside works: a Sonatype staging repository is one per build,
    // not one per module. Once there is a second (third etc.) published module, both artifacts have to
    // go into the same staging repository and be closed together.
    id("kolbasa.release")
}
