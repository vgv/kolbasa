plugins {
    java
}

// ===== Performance tests =====
// They live in the test source set, under src/test/kotlin/performance.
tasks.register<JavaExec>("performance") {
    mainClass = "performance.MainKt"
    classpath += java.sourceSets.getByName("test").runtimeClasspath
}
