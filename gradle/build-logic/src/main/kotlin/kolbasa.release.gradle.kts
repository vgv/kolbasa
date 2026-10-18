tasks.named("release") {
    // Every nebula release (final, devSnapshot) publishes to Maven Central. A snapshot is uploaded by the task
    // itself. A final release is uploaded and released at the very end of the build, after nebula pushes the git tag.
    dependsOn(":kolbasa:publishToMavenCentral")
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
