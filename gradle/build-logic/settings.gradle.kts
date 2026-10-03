// The root build's version catalog, so build logic and the project it builds agree on versions.
// Note: versionCatalogs only exists inside dependencyResolutionManagement, which is still
// @Incubating - that is where the "unstable API" warning comes from. Repositories are declared the
// plain, stable way in build.gradle.kts instead.
dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            from(files("../libs.versions.toml"))
        }
    }
}

rootProject.name = "build-logic"
