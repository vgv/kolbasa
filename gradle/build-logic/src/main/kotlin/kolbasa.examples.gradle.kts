plugins {
    java
}

// ===== Examples =====
// They live in the test source set, under src/test/kotlin/examples, most of them as a Kotlin/Java pair.
tasks.register<JavaExec>("example") {
    description = "Run a single example from src/test/kotlin/examples, either Kotlin or Java. Specify the example name " +
        "with -Pname=<ExampleName> and optionally the language with -Plang=java|kotlin (default: kotlin). " +
        "For example: ./gradlew example -Pname=FilterExample -Plang=java"

    val exampleName = project.providers.gradleProperty("name").orElse("SimpleExample")
    val exampleLang = project.providers.gradleProperty("lang").orElse("kotlin")

    // A Kotlin example is a top-level main() and compiles to `examples.<name>Kt`,
    // its Java twin is a plain class and has no 'Kt' suffix
    mainClass = exampleName.zip(exampleLang) { name, lang ->
        when (lang) {
            "java" -> "examples.$name"
            "kotlin" -> "examples.${name}Kt"
            else -> throw GradleException("Unknown -Plang=$lang, expected 'java' or 'kotlin'")
        }
    }
    classpath += java.sourceSets.getByName("test").runtimeClasspath
}
