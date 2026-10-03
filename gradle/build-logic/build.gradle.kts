plugins {
    // Compiles every *.gradle.kts in src/main/kotlin into a plugin whose id is the file name, so
    // kolbasa.testing.gradle.kts becomes id("kolbasa.testing") in the root build. It also puts the
    // Gradle API and the Kotlin DSL (type-safe accessors like `tasks`, `sourceSets`, `publishing`)
    // on the compile classpath of those scripts.
    `kotlin-dsl`
}

repositories {
    // Where this build's own dependencies come from: kotlin-stdlib and kotlin-reflect for kotlin-dsl,
    // plus the nexus-publish plugin below. The root build's repositories do not reach here.
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    // Why is this dependency needed?
    // 1) kolbasa.publishing.gradle.kts file uses "io.github.gradle-nexus.publish-plugin" plugin
    // 2) Gradle needs to find the plugin's marker annotation and classes to compile "kolbasa.publishing.gradle.kts" file
    // 3) "build-logic" folder plugins compile before the root build, so the root build's plugin classpath is not available yet
    // 4) So, we need to put plugin's marker annotation and classes (e.g. JAR file) on this build's compile classpath, so
    //    Gradle can compile "kolbasa.publishing.gradle.kts" file
    // 5) Important part – classpath to compile files in "build-logic" should be defined here,
    //    not in the "kolbasa.publishing.gradle.kts" (because it's just Kotlin script) and not in the root
    //    build (because it's not available yet)
    // 6) Gradle takes all dependencies from this build file and compiles *.gradle.kts files in "build-logic" folder with them
    //
    // Roughly speaking, put all dependencies to compile files in "build-logic" folder here
    implementation("io.github.gradle-nexus:publish-plugin:${libs.versions.nexus.get()}")
}
