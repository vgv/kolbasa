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
    compileOnly(libs.micrometer.core)

    // Test
    // Which PostgreSQL versions we test against, shared with the examples and the benchmarks
    // It also brings JUnit, Testcontainers and the AbstractPostgreSQLTest base class
    testImplementation(project(":test-support"))
    // Micrometer is compileOnly above, so the metrics tests need it on their own classpath. The Prometheus registry is
    // there to check the names Prometheus sees
    testImplementation(libs.micrometer.core)
    testImplementation(libs.micrometer.registry.prometheus)
}

// Kotlin settings
kotlin {
    // The oldest Kotlin we support, `kotlin-language` in gradle/libs.versions.toml
    val language = KotlinVersion.fromVersion(libs.versions.kotlin.language.get())

    jvmToolchain(17)
    // The standard library in our POM: the oldest we support, too. Without this line it is the compiler's version.
    // Gradle would raise an application's standard library to it, and an application on an older Kotlin would stop
    // compiling: its compiler reads a standard library at most one version newer
    coreLibrariesVersion = "${language.version}.0"
    compilerOptions {
        apiVersion = language
        languageVersion = language
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
