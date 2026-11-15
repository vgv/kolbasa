package kolbasa.spring.boot

import kolbasa.cluster.Cluster
import kolbasa.consumer.datasource.Consumer
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.datasource.Producer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.context.annotation.ImportCandidates
import org.springframework.boot.json.JsonParserFactory
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class KolbasaAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))

    @Test
    fun testImportsFile_ListsAutoConfiguration() {
        // Spring Boot finds auto-configurations through the imports file. With a typo there the starter does nothing,
        // and nothing fails
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader)

        assertTrue(KolbasaAutoConfiguration::class.java.name in candidates.candidates)
    }

    @Test
    fun testMetadataFile_DescribesEveryProperty() {
        // IDEs read this file to know the kolbasa.* properties. A property missing from it is shown as unknown, and one
        // that the code no longer reads is still suggested
        val parser = JsonParserFactory.getJsonParser()
        val described = javaClass.classLoader.getResources("META-INF/spring-configuration-metadata.json").toList()
            .map { it.readText() }
            .filter { "\"kolbasa." in it } // Spring Boot's jars have this file too
            // One line: without Jackson the parser is BasicJsonParser, and it reads an object in the middle of a list
            // as a string when there is a line break around it
            .map { it.replace(Regex("\\s*\\n\\s*"), "") }
            .flatMap { parser.parseMap(it)["properties"] as List<*> }
            .map { it as Map<*, *> }
            .associate { it["name"] to it["defaultValue"].toString() }

        // Every property the starter reads, with its default: matchIfMissing is what an absent property means
        val switches = (KolbasaAutoConfiguration::class.java.declaredClasses + KolbasaAutoConfiguration::class.java)
            .flatMap { it.declaredMethods.toList() }
            .mapNotNull { it.getAnnotation(ConditionalOnBooleanProperty::class.java) }
            .associate { (it.value + it.name).single() to it.matchIfMissing.toString() }

        // And the schema attempts at startup, read by SchemaStartupAttempts
        val startup = listOf(SchemaStartupAttempts.CLUSTER_SCHEMA_PREFIX).flatMap { prefix ->
            listOf(
                "$prefix.startup-attempts" to SchemaStartupAttempts.DEFAULT_ATTEMPTS.toString(),
                "$prefix.startup-retry-delay" to SchemaStartupAttempts.DEFAULT_RETRY_DELAY
            )
        }
        val read = switches + startup

        assertEquals(read, described)
    }

    @Test
    fun testContext_Starts() {
        contextRunner.run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context).hasSingleBean(KolbasaAutoConfiguration::class.java)
            assertThat(context).doesNotHaveBean(Producer::class.java)
            assertThat(context).doesNotHaveBean(Consumer::class.java)
            assertThat(context).doesNotHaveBean(Mutator::class.java)
            assertThat(context).doesNotHaveBean(Inspector::class.java)
            assertThat(context).doesNotHaveBean(Cluster::class.java)
        }
    }

    @Test
    fun testStandaloneSettingsWithoutDataSource_Fails() {
        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { KolbasaStandaloneSettings(emptyList()) })
            .run { context ->
                assertThat(context).hasFailed()
            }
    }
}
