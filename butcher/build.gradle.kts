import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)

    // JUnit settings and one Test task per supported PostgreSQL image
    id("kolbasa.testing")
}

repositories {
    mavenCentral()
}

dependencies {
    // butcher is an ordinary consumer of the library, it uses only public Kolbasa API
    implementation(project(":kolbasa"))

    implementation(libs.postgresql)

    // mordant (which clikt uses to talk to the terminal) ships three platform backends, and they are
    // independent choices:
    //
    // 1. mordant-jvm-graal-ffi is the GraalVM native-image backend. butcher is a plain JVM fat jar and never
    //    a native image, so it is dead weight (~34 KB) whatever the JDK.
    // 2. JNA vs FFM is the JDK-dependent pair and exactly one of them is needed. JNA runs on any JVM, FFM
    //    needs JDK 22+. We keep JNA because the toolchain here is JVM 17 - with FFM alone a 17 runtime has
    //    no backend at all and mordant degrades to a dumb terminal. Once butcher itself requires JDK 22+,
    //    swap the exclusion around and drop JNA instead, which takes ~1.9 MB of native libraries for some
    //    20 platforms out of the fat jar.
    implementation(libs.clikt) {
        exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-graal-ffi-jvm")
        exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-ffm-jvm")
        // exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-jna-jvm")
    }

    // Test
    // Which PostgreSQL versions we test against, shared with the library and the benchmarks
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
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}
