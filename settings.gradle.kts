pluginManagement {
    val kotlinVersion: String by settings

    plugins {
        id("java")
        id("org.jetbrains.kotlin.jvm") version kotlinVersion
    }

    repositories {
        gradlePluginPortal()
        mavenCentral()
    }

}
