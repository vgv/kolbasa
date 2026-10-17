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

    // A pool, not because butcher does anything concurrently, but because move-data takes a connection per
    // batch of 1000 rows. An unpooled DataSource pays a connection handshake for every batch
    implementation(libs.hikaricp)

    implementation(libs.logback.classic)

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

    // logback is a main dependency now, so the tests already have it
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        apiVersion = KotlinVersion.KOTLIN_2_1
        languageVersion = KotlinVersion.KOTLIN_2_1
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}

// ===== Butcher fat jar (build/libs/butcher.jar) =====
tasks.register<Jar>("butcherJar") {
    group = "build"
    description = "Builds the standalone butcher CLI fat jar"

    archiveBaseName.set("butcher")
    archiveClassifier.set("")
    archiveVersion.set("")

    // Land in the repository root's build/libs, not the module's: the release workflow refers to
    // build/libs/butcher.jar, and it should not have to follow the CLI from module to module.
    destinationDirectory = rootProject.layout.buildDirectory.dir("libs")

    // Reproducibility: same source -> same output bytes
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true

    manifest {
        attributes(
            "Main-Class" to "kolbasa.butcher.MainKt",
            "Implementation-Title" to "butcher",
            "Implementation-Version" to project.sanitizeVersion(),
            // postgresql 42.7.x is a Multi-Release jar: it ships JDK11-specialized classes under META-INF/versions/11/
            "Multi-Release" to true,
            // mordant's terminal backend goes through native code (JNA now, FFM later), and since JDK 24
            // a restricted call without permission warns - and will throw in a future release
            "Enable-Native-Access" to "ALL-UNNAMED",
        )
    }

    // Our own compiled classes + resources
    from(sourceSets.main.get().output)

    // Everything butcher needs at runtime, unpacked: the library, the driver and clikt. The library's own
    // compileOnly dependencies (prometheus, opentelemetry) are not on this classpath, so they stay out
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }
    })

    // Exclude per-dep MANIFEST.MF: each dep has its own; our `manifest { }`
    // block above wins regardless, but excluding explicitly is clearer.
    exclude("META-INF/MANIFEST.MF")
    // Exclude a root module-info.class: we're an executable, not a JPMS module
    exclude("/module-info.class")
    // Signature files of a signed dependency do not survive repackaging, and a leftover one makes the JVM
    // refuse the jar
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
