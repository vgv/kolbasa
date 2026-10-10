pluginManagement {
    // Our own convention plugins, see gradle/build-logic
    includeBuild("gradle/build-logic")

    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

include("kolbasa")
include("examples")
