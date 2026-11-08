package kolbasa.spring.boot

import kolbasa.consumer.datasource.Consumer
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.datasource.Producer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.annotation.ImportCandidates
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
    fun testContext_Starts() {
        contextRunner.run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context).hasSingleBean(KolbasaAutoConfiguration::class.java)
            assertThat(context).doesNotHaveBean(Producer::class.java)
            assertThat(context).doesNotHaveBean(Consumer::class.java)
            assertThat(context).doesNotHaveBean(Mutator::class.java)
            assertThat(context).doesNotHaveBean(Inspector::class.java)
        }
    }

    @Test
    fun testLocalSettingsWithoutDataSource_Fails() {
        contextRunner
            .withBean(LocalKolbasaSettings::class.java, { LocalKolbasaSettings(emptyList()) })
            .run { context ->
                assertThat(context).hasFailed()
            }
    }
}
