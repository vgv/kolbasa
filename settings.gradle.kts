rootProject.name = "kolbasa"

pluginManagement {
    // Our own convention plugins (kolbasa.testing, kolbasa.butcher, kolbasa.publishing)
    includeBuild("gradle/build-logic")

    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}
