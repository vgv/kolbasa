import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)

    // JUnit settings and one Test task per supported PostgreSQL image, shared with any other module
    // that gets a test suite. See gradle/build-logic/src/main/kotlin/kolbasa.testing.gradle.kts
    id("kolbasa.testing")
}

repositories {
    mavenCentral()
}

dependencies {
    // `api`: a test that extends AbstractPostgreSQLTest compiles against these itself (JUnit annotations,
    // pgContainer), so they are passed on to every module that depends on test-support
    api(libs.junit.jupiter)
    api(libs.testcontainers.postgresql)
    api(libs.mockk)


    // Only AbstractPostgreSQLTest uses these. A module that uses test-support still gets them at runtime,
    // but does not see them when compiling
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    implementation(libs.logback.core)
    implementation(libs.logback.classic)

    runtimeOnly(libs.junit.launcher)
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}
