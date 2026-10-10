import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)

    // JUnit settings and one Test task per supported PostgreSQL image, shared with any other module
    // that gets a test suite. See gradle/build-logic/src/main/kotlin/kolbasa.testing.gradle.kts
    id("kolbasa.testing")

    // Publishing to Maven Central: the publication, the POM and the signature.
    // Staging and release are repository-wide and live in the root build.
    signing
    `maven-publish`
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
    testImplementation(libs.prometheus.metrics.exporter.httpserver)
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

// =====================================================================================
java {
    withSourcesJar()
    withJavadocJar()
}

// All checks were already made by workflow "On pull request" => no checks here
if (gradle.startParameter.taskNames.contains("final")) {
    tasks.named("build").get().apply {
        dependsOn.removeIf { it == "check" }
    }
}

tasks.withType<Sign> {
    doFirst {
        SettingsProvider.validateGPGSecrets()
    }
    dependsOn(tasks.getByName("build"))
}

tasks.withType<PublishToMavenRepository> {
    doFirst {
        SettingsProvider.validateSonatypeCredentials()
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = SettingsProvider.ARTIFACT_GROUP_ID
            artifactId = SettingsProvider.ARTIFACT_NAME
            version = project.sanitizeVersion()
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("Kolbasa")
                description.set("A reliable message & job queue for Java & Kotlin, built on PostgreSQL.")
                url.set("https://github.com/vgv/kolbasa")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("vgv")
                        name.set("Vasily Vasilkov")
                        email.set("chand0s@yandex.ru")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/vgv/kolbasa.git")
                    developerConnection.set("scm:git:ssh://github.com:vgv/kolbasa.git")
                    url.set("https://github.com/vgv/kolbasa")
                }
            }
        }
    }
}

signing {
    useInMemoryPgpKeys(SettingsProvider.gpgSigningKey, SettingsProvider.gpgSigningPassword)
    sign(publishing.publications["mavenJava"])
}

// =====================================================================================
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
