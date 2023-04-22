plugins {
	id("java")
	id("org.jetbrains.kotlin.jvm")
}

apply(plugin = "java")
apply(plugin = "kotlin")

repositories {
	mavenCentral()
}

dependencies {
	val kotlinVersion: String by project
	val slf4jVersion: String by project
	val logbackVersion: String by project
	val postgresqlVersion: String by project
    val prometheusVersion: String by project
	val testContainersVersion: String by project
	val junitVersion: String by project
	val mockkVersion: String by project

	// Kotlin
	implementation("org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion")
	implementation("org.jetbrains.kotlin:kotlin-stdlib-common:$kotlinVersion")
	implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")

	// PostgreSQL
	implementation("org.postgresql:postgresql:$postgresqlVersion")

	// Logging
	implementation("org.slf4j:slf4j-api:$slf4jVersion")

    // Metrics
    implementation("io.prometheus:simpleclient:$prometheusVersion")

    // ---------------------------------------------------------------------------------

    // Test
	testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")

	testImplementation("io.mockk:mockk:$mockkVersion")

	testImplementation("org.testcontainers:testcontainers:$testContainersVersion")
	testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
	testImplementation("org.testcontainers:postgresql:$testContainersVersion")

	testImplementation("org.jetbrains.kotlin:kotlin-test:$kotlinVersion")
	testImplementation("org.jetbrains.kotlin:kotlin-test-common:$kotlinVersion")

    testImplementation("ch.qos.logback:logback-core:$logbackVersion")
    testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
    testImplementation("com.zaxxer:HikariCP:5.0.1")

}

tasks.withType<Test> {
	useJUnitPlatform()
}

// Kotlin settings
val compileKotlin: org.jetbrains.kotlin.gradle.tasks.KotlinCompile by tasks
compileKotlin.kotlinOptions.jvmTarget = "17"
compileKotlin.kotlinOptions.apiVersion = "1.7"
compileKotlin.kotlinOptions.languageVersion = "1.7"

val compileTestKotlin: org.jetbrains.kotlin.gradle.tasks.KotlinCompile by tasks
compileTestKotlin.kotlinOptions.jvmTarget = "17"
compileTestKotlin.kotlinOptions.apiVersion = "1.7"
compileTestKotlin.kotlinOptions.languageVersion = "1.7"
