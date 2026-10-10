import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)
}

repositories {
    mavenCentral()
}

dependencies {
    // Like the examples, the benchmarks are an ordinary consumer of the library: public API only.
    // Measuring through the same surface a user has is the whole point.
    implementation(project(":kolbasa"))

    // Which PostgreSQL version to start, decided once for the whole repository
    implementation(project(":test-support"))

    implementation(libs.testcontainers.postgresql)
    implementation(libs.hikaricp)
    implementation(libs.postgresql)

    // Testcontainers logs through SLF4J; without a binding every run starts with a warning
    runtimeOnly(libs.logback.classic)
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}

// ===== Running the benchmarks =====
tasks.register<JavaExec>("performance") {
    group = "application"
    description = "Run the benchmark suite"

    mainClass = "kolbasa.performance.MainKt"
    classpath = sourceSets.main.get().runtimeClasspath
}
