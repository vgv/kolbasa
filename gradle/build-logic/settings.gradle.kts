dependencyResolutionManagement {
    // The root build's version catalog: build logic and the project it builds then agree on versions.
    // A catalog belongs to one build, so an included build has to ask for it explicitly.
    versionCatalogs {
        create("libs") {
            from(files("../libs.versions.toml"))
        }
    }
}

rootProject.name = "build-logic"
