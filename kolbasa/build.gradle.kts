import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)

    // JUnit settings and one Test task per supported PostgreSQL image, shared with any other module
    // that gets a test suite. See gradle/build-logic/src/main/kotlin/kolbasa.testing.gradle.kts
    id("kolbasa.testing")

    // Publishing to Maven Central: the publication, the POM, the signature and the upload
    alias(libs.plugins.vanniktech.publish)
}

repositories {
    mavenCentral()
}

dependencies {
    // PostgreSQL
    implementation(libs.postgresql)

    // Metrics
    compileOnly(libs.prometheus.metrics.core)

    // OpenTelemetry
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.opentelemetry.sdk)
    compileOnly(libs.opentelemetry.semconv)
    compileOnly(libs.opentelemetry.instrumentation.api)
    compileOnly(libs.opentelemetry.instrumentation.api.incubator)

    // Test
    // Which PostgreSQL versions we test against, shared with the examples and the benchmarks
    testImplementation(project(":test-support"))

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
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        // We need JVM default methods for interfaces, but don't need the compatibility bridges
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
        freeCompilerArgs.add("-Xconsistent-data-class-copy-visibility")
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

// Credentials are Gradle properties; CI passes them as ORG_GRADLE_PROJECT_<name> environment variables:
// - mavenCentralUsername, mavenCentralPassword: a user token from https://central.sonatype.com/usertoken
// - signingInMemoryKey, signingInMemoryKeyPassword: an ascii-armored PGP secret key and its password
mavenPublishing {
    // Upload and release in one go, no manual step on the Central Portal.
    // A -SNAPSHOT version goes to the snapshot repository instead.
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    coordinates(SettingsProvider.ARTIFACT_GROUP_ID, SettingsProvider.ARTIFACT_NAME, project.sanitizeVersion())

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
