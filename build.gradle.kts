import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.nebula.release)

    // ----------------------------------------------------
    // Test settings, plus one `testPg_*` task per supported PostgreSQL version
    id("kolbasa.testing")
    // `performance` - runs the benchmark suite
    id("kolbasa.performance")
    // `example` - runs a single example from src/test/kotlin/examples, Kotlin or Java
    id("kolbasa.examples")
    // `butcherJar` - the standalone butcher CLI fat jar
    id("kolbasa.butcher")
    // Sources/javadoc jars, signing, publishing and releasing to Maven Central. Has to come after
    // nebula-release: it wires itself into its `final` / `devSnapshot` / `release` tasks
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

    // ------------------------------------------------------------------------
    // Butcher CLI, not part of the library's surface - see the kolbasa.butcher convention plugin
    butcherClasspath(libs.clikt)

    // ------------------------------------------------------------------------
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

// ===== Compilation and source layout =====
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

// Java examples live next to their Kotlin twins in src/test/kotlin, so javac has to look there too.
// Without this, a .java file in that directory is passed to kotlinc as a resolution-only Java root
// and is never compiled to bytecode by anything - not by `test`, not by CI.
sourceSets.test {
    java.srcDir("src/test/kotlin")
}
