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
    // It also brings JUnit, Testcontainers and the AbstractPostgreSQLTest base class
    testImplementation(project(":test-support"))

    // OpenTelemetry is compileOnly above, so the tracing tests bring it in themselves,
    // plus the SDK testing kit with its in-memory span exporter
    testImplementation(libs.opentelemetry.api)
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.opentelemetry.semconv)
    testImplementation(libs.opentelemetry.instrumentation.api)
    testImplementation(libs.opentelemetry.instrumentation.api.incubator)
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

// The POM fields that belong to this module, the rest comes from kolbasa.publishing
mavenPublishing {
    pom {
        name.set("Kolbasa")
        description.set("A reliable message & job queue for Java & Kotlin, built on PostgreSQL.")
    }
}
