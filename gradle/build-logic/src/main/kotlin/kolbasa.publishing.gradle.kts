// Publishing to Maven Central: the publication, the POM, the signature and the upload. Shared by every published
// module. A module adds only what is its own, the POM name and description, in its own mavenPublishing { pom { } }
//
// Credentials are Gradle properties; CI passes them as ORG_GRADLE_PROJECT_<name> environment variables:
// - mavenCentralUsername, mavenCentralPassword: a user token from https://central.sonatype.com/usertoken
// - signingInMemoryKey, signingInMemoryKeyPassword: an ascii-armored PGP secret key and its password

plugins {
    id("com.vanniktech.maven.publish")
}

// The root release task publishes only the modules from this list, see kolbasa.release.gradle.kts
check(project.name in SettingsProvider.ARTIFACT_NAMES) {
    "Module '${project.name}' is published, but it is not in SettingsProvider.ARTIFACT_NAMES, so a release would skip it"
}

mavenPublishing {
    // Upload and release in one go, no manual step on the Central Portal.
    // A -SNAPSHOT version goes to the snapshot repository instead.
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    coordinates(SettingsProvider.ARTIFACT_GROUP_ID, project.name, project.sanitizeVersion())

    pom {
        url.set("https://github.com/vgv/kolbasa")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("vgv")
                name.set("Vasily Vasilkov")
                email.set("chand0s@yandex.ru")
            }
        }
        scm {
            connection.set("scm:git:git://github.com/vgv/kolbasa.git")
            developerConnection.set("scm:git:ssh://github.com:vgv/kolbasa.git")
            url.set("https://github.com/vgv/kolbasa")
        }
    }
}

// =====================================================================================
// All checks were already made by workflow "On pull request" => no checks here
if (gradle.startParameter.taskNames.contains("final")) {
    tasks.named("build").get().apply {
        dependsOn.removeIf { it == "check" }
    }
}

tasks.withType<Sign> {
    dependsOn(tasks.getByName("build"))
}
