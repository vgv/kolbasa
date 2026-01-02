import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    java
    alias(libs.plugins.kotlin.jvm)
    signing
    `maven-publish`
    alias(libs.plugins.nebula.release)
    alias(libs.plugins.nexus.publish)
}

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin
    implementation(libs.kotlin.stdlib)
    implementation(libs.kotlin.reflect)

    // PostgreSQL
    implementation(libs.postgresql)

    // JCommander
    implementation(libs.jcommander)

    // Metrics
    compileOnly(libs.prometheus.metrics.core)

    // OpenTelemetry
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.opentelemetry.sdk)
    compileOnly(libs.opentelemetry.semconv)
    compileOnly(libs.opentelemetry.instrumentation.api)
    compileOnly(libs.opentelemetry.instrumentation.api.incubator)
    // ---------------------------------------------------------------------------------

    // Test
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)

    testImplementation(libs.mockk)

    testImplementation(libs.testcontainers.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)

    testImplementation(libs.logback.core)
    testImplementation(libs.logback.classic)
    testImplementation(libs.hikaricp)
    testImplementation(libs.prometheus.metrics.exporter.httpserver)
}

// Kotlin settings
kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_1_9
        languageVersion = KotlinVersion.KOTLIN_1_9
        // We need JVM default methods for interfaces, but don't need the compatibility bridges
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
        freeCompilerArgs.add("-Xconsistent-data-class-copy-visibility")
    }
}

// Performance tests
tasks.register<JavaExec>("performance") {
    mainClass = "performance.MainKt"
    classpath += java.sourceSets.getByName("test").runtimeClasspath
}

// Examples
tasks.register<JavaExec>("example") {
    val propertyName = "name"
    val propertyValue = project.providers.gradleProperty(propertyName)
    val exampleName = if (propertyValue.isPresent) {
        propertyValue.get()
    } else {
        "SimpleExample"
    }

    mainClass = "examples.${exampleName}Kt"
    classpath += java.sourceSets.getByName("test").runtimeClasspath
}

// Unit tests settings
tasks.withType<Test> {
    // enable parallel tests execution
    systemProperties["junit.jupiter.execution.parallel.enabled"] = true
    systemProperties["junit.jupiter.execution.parallel.mode.default"] = "concurrent"

    // JUnit settings
    useJUnitPlatform {
        enableAssertions = true
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
            events = setOf(TestLogEvent.FAILED, TestLogEvent.SKIPPED)
            showStandardStreams = false
        }
    }
}

java {
    withSourcesJar()
    withJavadocJar()
}

val settingsProvider = SettingsProvider()

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
        settingsProvider.validateGPGSecrets()
    }
    dependsOn(tasks.getByName("build"))
}

tasks.withType<PublishToMavenRepository> {
    doFirst {
        settingsProvider.validateSonatypeCredentials()
    }
}

tasks.register("printFinalReleaseNote") {
    doLast {
        printFinalReleaseNote(
            groupId = "io.github.vgv",
            artifactId = "kolbasa",
            sanitizedVersion = project.sanitizeVersion()
        )
    }
    dependsOn(tasks.getByName("final"))
}

tasks.register("printDevSnapshotReleaseNote") {
    doLast {
        printDevSnapshotReleaseNote(
            groupId = "io.github.vgv",
            artifactId = "kolbasa",
            sanitizedVersion = project.sanitizeVersion()
        )
    }
    dependsOn(tasks.getByName("devSnapshot"))
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = "io.github.vgv"
            artifactId = "kolbasa"
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
                description.set("Kotlin library for PostgreSQL queues")
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
    useInMemoryPgpKeys(settingsProvider.gpgSigningKey, settingsProvider.gpgSigningPassword)
    sign(publishing.publications["mavenJava"])
}

nexusPublishing {
    repositories {
        sonatype {
            useStaging.set(!project.isSnapshotVersion())
            packageGroup.set("io.github.vgv")
            username = settingsProvider.sonatypeUsername
            password = settingsProvider.sonatypePassword
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
        }
    }
}

// We want to change SNAPSHOT versions format from:
// 		<major>.<minor>.<patch>-dev.#+<branchname>.<hash> (local branch)
// 		<major>.<minor>.<patch>-dev.#+<hash> (github pull request)
// to:
// 		<major>.<minor>.<patch>-dev+<branchname>-SNAPSHOT
fun Project.sanitizeVersion(): String {
    val version = version.toString()
    return if (project.isSnapshotVersion()) {
        val githubHeadRef = settingsProvider.githubHeadRef
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
    println("Edit or delete artifacts on OSS Nexus Repository Manager:")
    println("	https://oss.sonatype.org/#nexus-search;gav~$groupId~~~~")
    println()
    println("Control staging repositories on OSS Nexus Repository Manager:")
    println("	https://oss.sonatype.org/#stagingRepositories")
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
    println("	https://central.sonatype.com/repository/maven-snapshots/${groupId.replace('.', '/')}/$artifactId/$sanitizedVersion/maven-metadata.xml")
    println()
    println("========================================================")
    println()
}

class SettingsProvider {

    val gpgSigningKey: String?
        get() = System.getenv(GPG_SIGNING_KEY_PROPERTY)

    val gpgSigningPassword: String?
        get() = System.getenv(GPG_SIGNING_PASSWORD_PROPERTY)

    val sonatypeUsername: String?
        get() = System.getenv(SONATYPE_USERNAME_PROPERTY)

    val sonatypePassword: String?
        get() = System.getenv(SONATYPE_PASSWORD_PROPERTY)

    val githubHeadRef: String?
        get() = System.getenv(GITHUB_HEAD_REF_PROPERTY)

    fun validateGPGSecrets() = require(
        value = !gpgSigningKey.isNullOrBlank() && !gpgSigningPassword.isNullOrBlank(),
        lazyMessage = { "Both $GPG_SIGNING_KEY_PROPERTY and $GPG_SIGNING_PASSWORD_PROPERTY environment variables must not be empty" }
    )

    fun validateSonatypeCredentials() = require(
        value = !sonatypeUsername.isNullOrBlank() && !sonatypePassword.isNullOrBlank(),
        lazyMessage = { "Both $SONATYPE_USERNAME_PROPERTY and $SONATYPE_PASSWORD_PROPERTY environment variables must not be empty" }
    )

    private companion object {
        // it should be a so-called "ascii-armored in-memory PGP secret key"
        private const val GPG_SIGNING_KEY_PROPERTY = "GPG_SIGNING_KEY"
        private const val GPG_SIGNING_PASSWORD_PROPERTY = "GPG_SIGNING_PASSWORD"
        // generate a name/password at https://central.sonatype.com/usertoken
        private const val SONATYPE_USERNAME_PROPERTY = "SONATYPE_USERNAME"
        private const val SONATYPE_PASSWORD_PROPERTY = "SONATYPE_PASSWORD"
        private const val GITHUB_HEAD_REF_PROPERTY = "GITHUB_HEAD_REF"
    }
}
