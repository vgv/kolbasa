plugins {
    java
}

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

    // All runtime dependencies, unpacked. compileOnly deps (prometheus,
    // opentelemetry) are deliberately NOT in runtimeClasspath, so they
    // don't end up in the fat jar.
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }
    })

    // Exclude per-dep MANIFEST.MF: each dep has its own; our `manifest { }`
    // block above wins regardless, but excluding explicitly is clearer.
    exclude("META-INF/MANIFEST.MF")
    // Exclude a root module-info.class: we're an executable, not a JPMS module
    exclude("/module-info.class")

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
