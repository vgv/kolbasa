import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)
}

repositories {
    mavenCentral()
}

dependencies {
    // The examples are an ordinary consumer of the library: public API only. If an example needs an
    // `internal` declaration to compile, it is not an example anyone could copy into their own project.
    implementation(project(":kolbasa"))

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

// Every example comes as a Kotlin/Java pair in the same directory, so javac has to look into the Kotlin
// source root as well - otherwise the .java twins are never compiled by anything.
sourceSets.main {
    java.srcDir("src/main/kotlin")
}

// ===== Running a single example =====
tasks.register<JavaExec>("example") {
    group = "application"
    description = "Run a single example, either Kotlin or Java. Specify the example name with " +
        "-Pname=<ExampleName> and optionally the language with -Plang=java|kotlin (default: kotlin). " +
        "For example: ./gradlew example -Pname=FilterExample -Plang=java"

    val exampleName = project.providers.gradleProperty("name").orElse("SimpleExample")
    val exampleLang = project.providers.gradleProperty("lang").orElse("kotlin")

    // A Kotlin example is a top-level main() and compiles to `kolbasa.example.<name>Kt`,
    // its Java twin is a plain class and has no suffix
    mainClass = exampleName.zip(exampleLang) { name, lang ->
        when (lang) {
            "java" -> "kolbasa.example.$name"
            "kotlin" -> "kolbasa.example.${name}Kt"
            else -> throw GradleException("Unknown -Plang=$lang, expected 'java' or 'kotlin'")
        }
    }
    classpath = sourceSets.main.get().runtimeClasspath
}
