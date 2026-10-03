plugins {
    java
    signing
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin")
}

// Sources and javadoc are part of what we publish
java {
    withSourcesJar()
    withJavadocJar()
}

// ===== Release wiring =====
// Note: `final`, `devSnapshot` and `release` come from the nebula-release plugin, which the root
// build applies before this one, so the tasks already exist by the time we look them up.
tasks {
    // All checks were already made by workflow "On pull request" => no checks here
    if (gradle.startParameter.taskNames.contains("final")) {
        named("build").get().apply {
            dependsOn.removeIf { it == "check" }
        }
    }

    afterEvaluate {
        // Publish artifacts to Maven Central before pushing new git tag to repo
        named("release").get().apply {
            dependsOn(named("publishToSonatype").get())
        }

        named("closeAndReleaseStagingRepositories").get().apply {
            dependsOn(named("final").get())
        }
    }
}

tasks.withType<Sign> {
    doFirst {
        SettingsProvider.validateGPGSecrets()
    }
    dependsOn(tasks.getByName("build"))
}

tasks.withType<PublishToMavenRepository> {
    doFirst {
        SettingsProvider.validateSonatypeCredentials()
    }
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

// ===== Publishing =====
publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = SettingsProvider.ARTIFACT_GROUP_ID
            artifactId = SettingsProvider.ARTIFACT_NAME
            version = project.sanitizeVersion()
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("Kolbasa")
                description.set("A reliable message & job queue for Java & Kotlin, built on PostgreSQL.")
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
    }
}

signing {
    useInMemoryPgpKeys(SettingsProvider.gpgSigningKey, SettingsProvider.gpgSigningPassword)
    sign(publishing.publications["mavenJava"])
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
