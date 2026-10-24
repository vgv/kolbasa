import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)

    // JUnit settings and one Test task per supported PostgreSQL image, shared with any other module
    // that gets a test suite. See gradle/build-logic/src/main/kotlin/kolbasa.testing.gradle.kts
    id("kolbasa.testing")

    // Publishing to Maven Central, shared with every published module.
    // See gradle/build-logic/src/main/kotlin/kolbasa.publishing.gradle.kts
    id("kolbasa.publishing")
}

repositories {
    mavenCentral()
}

dependencies {
    // The tracing hook this module implements
    api(project(":kolbasa"))

    // OpenTelemetryTracing takes an OpenTelemetry instance, so the API is a part of this module's API
    api(libs.opentelemetry.api)

    // Used only inside
    implementation(libs.opentelemetry.instrumentation.api)
    implementation(libs.opentelemetry.instrumentation.api.incubator)

    // Test
    // Which PostgreSQL versions we test against, plus JUnit, Testcontainers and the AbstractPostgreSQLTest base class
    testImplementation(project(":test-support"))

    // The SDK and its testing kit with the in-memory span exporter
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
}

// Kotlin settings
kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        // We need JVM default methods for interfaces, but don't need the compatibility bridges
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}

// The POM fields that belong to this module, the rest comes from kolbasa.publishing
mavenPublishing {
    pom {
        name.set("Kolbasa OpenTelemetry")
        description.set("OpenTelemetry tracing for Kolbasa, a message & job queue for Java & Kotlin built on PostgreSQL.")
    }
}
