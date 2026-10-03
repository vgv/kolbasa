plugins {
    java
}

// ===== Butcher CLI dependencies =====
// Create separate configuration butherClasspath to manage butcher dependencies separately
val butcherClasspath = configurations.create("butcherClasspath") {
    // Declarable and resolvable, but it exposes nothing: Gradle will refuse an attempt to publish artifacts from it
    isCanBeConsumed = false

    // mordant talks to the terminal (size, TTY detection, colors on Windows) through a platform
    // backend, and ships three of them. They are independent choices, so keep them apart:
    //
    // 1. mordant-jvm-graal-ffi is the GraalVM native-image backend. butcher is a plain JVM fat jar
    //    and never a native image, so this one is dead weight (~34 KB) regardless of the JDK:
    exclude(mapOf("group" to "com.github.ajalt.mordant", "module" to "mordant-jvm-graal-ffi-jvm"))

    // 2. JNA vs FFM is the JDK-dependent, exactly one of them is needed. JNA runs on any JVM; FFM needs JDK 22+.
    // Keep JNA because our toolchain is JVM 17 - with FFM alone, a 17 runtime has no backend at all and mordant degrades
    // to a dumb terminal. Once butcher itself requires JDK 22+, swap the exclusion the other way around: drop JNA instead,
    // which takes ~1.9 MB of native libraries for ~20 platforms out of the fat jar
    // exclude(mapOf("group" to "com.github.ajalt.mordant", "module" to "mordant-jvm-jna-jvm"))
}

// Butcher sources sit in the main source set, so the compiler needs those dependencies as well
configurations.named("compileOnly") { extendsFrom(butcherClasspath) }

// ===== Butcher fat jar (build/libs/butcher.jar) =====
tasks.register<Jar>("butcherJar") {
    group = "build"
    description = "Builds the standalone butcher CLI fat jar"

    archiveBaseName.set("butcher")
    archiveClassifier.set("")
    archiveVersion.set("")

    // Reproducibility: same source → same output bytes
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true

    manifest {
        attributes(
            "Main-Class" to "kolbasa.cluster.butcher.ButcherKt",
            "Implementation-Title" to "butcher",
            "Implementation-Version" to project.sanitizeVersion(),
            // postgresql 42.7.x is a Multi-Release jar: it ships JDK11-specialized classes under META-INF/versions/11/
            "Multi-Release" to true,
        )
    }

    // Our own compiled classes + resources.
    from(sourceSets.main.get().output)
    // All runtime and butcher dependencies, unpacked. compileOnly deps (prometheus, opentelemetry) are
    // deliberately NOT in runtimeClasspath, so they don't end up in the fat jar
    from({ configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) } })
    from({ butcherClasspath.map { if (it.isDirectory) it else zipTree(it) } })

    // Exclude per-dep MANIFEST.MF: each dep has its own; our `manifest { }`
    // block above wins regardless, but excluding explicitly is clearer.
    exclude("META-INF/MANIFEST.MF")
    // Exclude a root module-info.class: we're an executable, not a JPMS module
    exclude("/module-info.class")

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
