import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)
}

repositories {
    mavenCentral()
}

// Not published: this module exists so that the library's tests, the examples and the benchmarks agree
// on which PostgreSQL versions they run against, without each of them keeping its own copy of the list.
// It deliberately has no dependencies - it owns one resource file and the code that parses it.

kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}
