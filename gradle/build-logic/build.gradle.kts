plugins {
    // Compiles every *.gradle.kts in src/main/kotlin into a plugin whose id is the file name, so
    // kolbasa.testing.gradle.kts becomes id("kolbasa.testing") in the root build. It also puts the
    // Gradle API and the Kotlin DSL (type-safe accessors like `tasks`, `sourceSets`, `publishing`)
    // on the compile classpath of those scripts.
    `kotlin-dsl`
}

repositories {
    // Where this build's own dependencies come from: kotlin-stdlib and kotlin-reflect for kotlin-dsl.
    // The root build's repositories do not reach here.
    mavenCentral()
}
