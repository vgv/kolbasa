import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

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
    val kotlinVersion: String by project
    val postgresqlVersion: String by project
    val prometheusVersion: String by project
    val junitVersion: String by project
    val mockkVersion: String by project
    val testContainersVersion: String by project
    val logbackVersion: String by project
    val hikariVersion: String by project
    val openTelemetryVersion: String by project
    val openTelemetryInstrumentationVersion: String by project
    val openTelemetryInstrumentationIncubatorVersion: String by project
    val openTelemetrySemconvVersion: String by project

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-common:$kotlinVersion")
    implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")

    // PostgreSQL
    implementation("org.postgresql:postgresql:$postgresqlVersion")

    // Metrics
    compileOnly("io.prometheus:prometheus-metrics-core:$prometheusVersion")

    // OpenTelemetry
    compileOnly("io.opentelemetry:opentelemetry-api:$openTelemetryVersion")
    compileOnly("io.opentelemetry:opentelemetry-sdk:$openTelemetryVersion")
    compileOnly("io.opentelemetry:opentelemetry-sdk-trace:$openTelemetryVersion")
    compileOnly("io.opentelemetry:opentelemetry-exporter-otlp:$openTelemetryVersion")
    compileOnly("io.opentelemetry.semconv:opentelemetry-semconv:$openTelemetrySemconvVersion")
    compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-api:$openTelemetryInstrumentationVersion")
    compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-api-incubator:$openTelemetryInstrumentationIncubatorVersion")
    // ---------------------------------------------------------------------------------

    // Test
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")

    testImplementation("io.mockk:mockk:$mockkVersion")

    testImplementation("org.testcontainers:testcontainers:$testContainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
    testImplementation("org.testcontainers:postgresql:$testContainersVersion")

    testImplementation("org.jetbrains.kotlin:kotlin-test:$kotlinVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-common:$kotlinVersion")

    testImplementation("ch.qos.logback:logback-core:$logbackVersion")
    testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
    testImplementation("com.zaxxer:HikariCP:$hikariVersion")
    testImplementation("io.prometheus:prometheus-metrics-exporter-httpserver:$prometheusVersion")
}

// Kotlin settings
tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
    kotlinOptions.apiVersion = "1.7"
    kotlinOptions.languageVersion = "1.7"
    // to generate default method implementations in interfaces
    kotlinOptions.freeCompilerArgs = listOf("-Xjvm-default=all")
}

// Performance tests
task<JavaExec>("performance") {
    mainClass = "performance.MainKt"
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

        named("closeAndReleaseStagingRepository").get().apply {
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
            username.set(settingsProvider.sonatypeUsername)
            password.set(settingsProvider.sonatypePassword)
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
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
            // github pull request
            version
                .replace(Regex("-dev\\.\\d+\\+[a-f0-9]+$"), "-dev+$githubHeadRef-SNAPSHOT")
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
    println("	https://s01.oss.sonatype.org/content/repositories/snapshots/${groupId.replace('.', '/')}/$artifactId/")
    println()
    println("Edit or delete artifacts on OSS Nexus Repository Manager:")
    println("	https://s01.oss.sonatype.org/#nexus-search;gav~$groupId~~~~")
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
        private const val GPG_SIGNING_KEY_PROPERTY = "GPG_SIGNING_KEY"
        private const val GPG_SIGNING_PASSWORD_PROPERTY = "GPG_SIGNING_PASSWORD"
        private const val SONATYPE_USERNAME_PROPERTY = "SONATYPE_USERNAME"
        private const val SONATYPE_PASSWORD_PROPERTY = "SONATYPE_PASSWORD"
        private const val GITHUB_HEAD_REF_PROPERTY = "GITHUB_HEAD_REF"
    }
}
