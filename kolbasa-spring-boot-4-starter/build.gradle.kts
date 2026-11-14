import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.kotlin.jvm)

    // JUnit settings and one Test task per supported PostgreSQL image, shared with any other module
    // that gets a test suite. See gradle/build-logic/src/main/kotlin/kolbasa.testing.gradle.kts
    id("kolbasa.testing")
}

repositories {
    mavenCentral()
}

dependencies {
    // The library this starter configures
    api(project(":kolbasa"))

    // Auto-configuration support. Every Spring Boot application has it already
    implementation(libs.spring.boot.autoconfigure)

    // @DependsOnDatabaseInitialization: the queue tables are created after Flyway, Liquibase and schema.sql. Every
    // application on Spring Boot's JDBC support has it already, through spring-boot-jdbc
    implementation(libs.spring.boot.sql)

    // Test
    // Which PostgreSQL versions we test against, plus JUnit, Testcontainers and the AbstractPostgreSQLTest base class
    testImplementation(project(":test-support"))

    // ApplicationContextRunner, and AssertJ: the runner's context assertions are built on it
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.assertj.core)
}

// Kotlin settings
kotlin {
    // The oldest Kotlin we support, `kotlin-language` in gradle/libs.versions.toml
    val language = KotlinVersion.fromVersion(libs.versions.kotlin.language.get())

    // The oldest Java we support, `java` in gradle/libs.versions.toml
    jvmToolchain(libs.versions.java.get().toInt())
    // The standard library in our POM: the oldest we support, too. Without this line it is the compiler's version.
    // Gradle would raise an application's standard library to it, and an application on an older Kotlin would stop
    // compiling: its compiler reads a standard library at most one version newer
    coreLibrariesVersion = "${language.version}.0"
    compilerOptions {
        apiVersion = language
        languageVersion = language
        // We need JVM default methods for interfaces, but don't need the compatibility bridges
        jvmDefault = JvmDefaultMode.NO_COMPATIBILITY
    }
}
