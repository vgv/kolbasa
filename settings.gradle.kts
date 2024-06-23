rootProject.name = "kolbasa"

pluginManagement {
    val kotlinVersion: String by settings
    val nexusPublishVersion: String by settings
    val nebulaReleaseVersion: String by settings

    plugins {
        id("org.jetbrains.kotlin.jvm") version kotlinVersion
        id("io.github.gradle-nexus.publish-plugin") version nexusPublishVersion
        id("com.netflix.nebula.release") version nebulaReleaseVersion
    }

    repositories {
        gradlePluginPortal()
        mavenCentral()
    }

}
