import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
}

// ===== Unit tests settings =====
tasks.withType<Test> {
    enableAssertions = true

    // enable parallel tests execution
    systemProperties["junit.jupiter.execution.parallel.enabled"] = true
    systemProperties["junit.jupiter.execution.parallel.mode.default"] = "concurrent"

    useJUnitPlatform()

    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        events = setOf(TestLogEvent.FAILED, TestLogEvent.SKIPPED)
        showStandardStreams = false
    }
}

// ===== Run the test suite against PostgreSQL versions =====
// One visible Test task per image, so any specific version can be run directly (e.g. ./gradlew testPg_15_8).
val pgImagesFile = rootProject.file("test-support/src/main/resources/postgresql-test-images.txt")
val pgImages: List<String> = pgImagesFile.readLines()
    .map(String::trim)
    .filter { it.isNotEmpty() && !it.startsWith("#") }
    .also { require(it.isNotEmpty()) { "${pgImagesFile.name} is empty or unreadable" } }

val testSources = sourceSets["test"]
val pgTasksByImage: Map<String, TaskProvider<Test>> = pgImages.associateWith { image ->
    val suffix = image.substringAfter(':').removeSuffix("-alpine").replace('.', '_') // 16.4-alpine -> 16_4
    tasks.register<Test>("testPg_$suffix") {
        group = "verification"
        description = "Run all tests on $image"
        // A hand-registered Test task does NOT inherit the test source set wiring that the built-in
        // `test` task gets, so set it explicitly (this also wires the test-compile dependency):
        testClassesDirs = testSources.output.classesDirs
        classpath = testSources.runtimeClasspath
        systemProperty("kolbasa.test.postgresql.image", image)
        outputs.upToDateWhen { false } // always actually run in a matrix
    }
}

// "Boundary" = the first and last patch of each major release, discovered from the image list.
val boundaryPgImages: List<String> = pgImages
    .groupBy { pgVersion(it).first }
    .flatMap { (_, images) -> listOf(images.minBy { pgVersion(it).second }, images.maxBy { pgVersion(it).second }) }
    .distinct() // a major with a single patch would otherwise appear twice

tasks.register("testBoundaryPgVersions") {
    group = "verification"
    description = "Run @unit-db tests against the first and last patch of each major PostgreSQL version."
    dependsOn(boundaryPgImages.map { pgTasksByImage.getValue(it) })
}

fun pgVersion(image: String): Pair<Int, Int> {
    // "postgres:16.4-alpine" -> (16, 4)
    val (major, minor) = image.substringAfter(':').substringBefore('-').split('.').map(String::toInt)
    return major to minor
}
