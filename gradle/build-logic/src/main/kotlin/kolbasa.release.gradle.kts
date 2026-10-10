plugins {
    // Root-only: the staging-repository lifecycle tasks live here, and publishToSonatype aggregates the
    // publications of whichever subprojects have one
    id("io.github.gradle-nexus.publish-plugin")
}

tasks.named("release") {
    // Publish artifacts to Maven Central before pushing new git tag to repo
    dependsOn(":kolbasa:publishToSonatype")
}

tasks.named("closeAndReleaseStagingRepositories") {
    dependsOn("final")
}

tasks.register("printFinalReleaseNote") {
    doLast {
        printFinalReleaseNote(
            groupId = SettingsProvider.ARTIFACT_GROUP_ID,
            artifactId = SettingsProvider.ARTIFACT_NAME,
            sanitizedVersion = project.sanitizeVersion()
        )
    }
    dependsOn(tasks.getByName("final"))
}

tasks.register("printDevSnapshotReleaseNote") {
    doLast {
        printDevSnapshotReleaseNote(
            groupId = SettingsProvider.ARTIFACT_GROUP_ID,
            artifactId = SettingsProvider.ARTIFACT_NAME,
            sanitizedVersion = project.sanitizeVersion()
        )
    }
    dependsOn(tasks.getByName("devSnapshot"))
}

nexusPublishing {
    repositories {
        sonatype {
            useStaging.set(!project.isSnapshotVersion())
            packageGroup.set(SettingsProvider.ARTIFACT_GROUP_ID)
            username = SettingsProvider.sonatypeUsername
            password = SettingsProvider.sonatypePassword
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
        }
    }
}
