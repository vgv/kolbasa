tasks.named("release") {
    // Every nebula release (final, devSnapshot) publishes to Maven Central. A snapshot is uploaded by the task
    // itself. A final release is uploaded and released at the very end of the build, after nebula pushes the git tag.
    SettingsProvider.ARTIFACT_NAMES.forEach { artifactId ->
        dependsOn(":$artifactId:publishToMavenCentral")
    }
}

tasks.register("printFinalReleaseNote") {
    doLast {
        SettingsProvider.ARTIFACT_NAMES.forEach { artifactId ->
            printFinalReleaseNote(
                groupId = SettingsProvider.ARTIFACT_GROUP_ID,
                artifactId = artifactId,
                sanitizedVersion = project.sanitizeVersion()
            )
        }
    }
    dependsOn(tasks.getByName("final"))
}

tasks.register("printDevSnapshotReleaseNote") {
    doLast {
        SettingsProvider.ARTIFACT_NAMES.forEach { artifactId ->
            printDevSnapshotReleaseNote(
                groupId = SettingsProvider.ARTIFACT_GROUP_ID,
                artifactId = artifactId,
                sanitizedVersion = project.sanitizeVersion()
            )
        }
    }
    dependsOn(tasks.getByName("devSnapshot"))
}
