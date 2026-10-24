pluginManagement {
    // Our own convention plugins, see gradle/build-logic
    includeBuild("gradle/build-logic")

    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

include("kolbasa")
include("kolbasa-opentelemetry")
include("test-support")
include("examples")
include("performance")
include("butcher")
