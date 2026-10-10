import org.gradle.api.Project

// We want to change SNAPSHOT versions format from:
// 		<major>.<minor>.<patch>-dev.#+<branchname>.<hash> (local branch)
// 		<major>.<minor>.<patch>-dev.#+<hash> (github pull request)
// to:
// 		<major>.<minor>.<patch>-dev+<branchname>-SNAPSHOT
fun Project.sanitizeVersion(): String {
    val version = version.toString()
    return if (project.isSnapshotVersion()) {
        val githubHeadRef = SettingsProvider.githubHeadRef
        if (githubHeadRef != null) {
            // GitHub pull request
            // githubHeadRef contains branch name, but branch name can have '/',
            // for example 'dependabot/gradle/com.netflix.nebula.release-20.2.0'
            // Maven Central doesn't allow '/' in artifact version, so we replace it with '-'
            val branchName = githubHeadRef.replace('/', '-')
            version.replace(Regex("-dev\\.\\d+\\+[a-f0-9]+$"), "-dev+$branchName-SNAPSHOT")
        } else {
            // local branch
            version
                .replace(Regex("-dev\\.\\d+\\+"), "-dev+")
                .replace(Regex("\\.[a-f0-9]+$"), "-SNAPSHOT")
        }
    } else {
        version
    }
}

fun Project.isSnapshotVersion() = version.toString().contains("-dev.")

fun printFinalReleaseNote(groupId: String, artifactId: String, sanitizedVersion: String) {
    println()
    println("========================================================")
    println()
    println("New RELEASE artifact version were published:")
    println("	groupId: $groupId")
    println("	artifactId: $artifactId")
    println("	version: $sanitizedVersion")
    println()
    println("Discover on Maven Central:")
    println("	https://repo1.maven.org/maven2/${groupId.replace('.', '/')}/$artifactId/")
    println()
    println("View on Central Portal:")
    println("	https://central.sonatype.com/artifact/$groupId/$artifactId/$sanitizedVersion")
    println()
    println("========================================================")
    println()
}

fun printDevSnapshotReleaseNote(groupId: String, artifactId: String, sanitizedVersion: String) {
    println()
    println("========================================================")
    println()
    println("New developer SNAPSHOT artifact version were published:")
    println("	groupId: $groupId")
    println("	artifactId: $artifactId")
    println("	version: $sanitizedVersion")
    println()
    println("Discover on Maven Central:")
    println(
        "	https://central.sonatype.com/repository/maven-snapshots/${
            groupId.replace(
                '.',
                '/'
            )
        }/$artifactId/$sanitizedVersion/maven-metadata.xml"
    )
    println()
    println("========================================================")
    println()
}
