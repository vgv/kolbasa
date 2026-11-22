package kolbasa.spring.boot

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kolbasa.telemetry.metrics.MetricsConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class KolbasaMetricsTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))

    @Test
    fun testAfterNames_RealClasses() {
        // Spring Boot skips a name it can't find, so a typo would quietly break the order
        assertEquals(MetricsAutoConfiguration::class.java.name, METRICS_AUTO_CONFIGURATION)
        assertEquals(
            CompositeMeterRegistryAutoConfiguration::class.java.name,
            COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION
        )
    }

    @Test
    fun testSpringBootMetrics_MetricsConfigOverItsRegistry() {
        // Spring Boot makes the MeterRegistry in its own auto-configurations, so kolbasa's must run after them
        contextRunner
            .withConfiguration(
                AutoConfigurations.of(
                    MetricsAutoConfiguration::class.java,
                    CompositeMeterRegistryAutoConfiguration::class.java,
                    SimpleMetricsExportAutoConfiguration::class.java
                )
            )
            .run { context ->
                assertThat(context).hasNotFailed()
                val registry = context.getBean(MeterRegistry::class.java)
                assertEquals(MetricsConfig.Config(registry), context.getBean(MetricsConfig::class.java))
            }
    }

    @Test
    fun testOwnRegistry_MetricsConfigOverIt() {
        val registry = SimpleMeterRegistry()

        contextRunner
            .withBean(MeterRegistry::class.java, { registry })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertEquals(MetricsConfig.Config(registry), context.getBean(MetricsConfig::class.java))
            }
    }

    @Test
    fun testNoRegistry_NoMetricsConfig() {
        contextRunner.run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context).doesNotHaveBean(MetricsConfig::class.java)
        }
    }

    @Test
    fun testOwnMetricsConfig_ReplacesStarters() {
        contextRunner
            .withBean(MeterRegistry::class.java, { SimpleMeterRegistry() })
            .withBean(MetricsConfig::class.java, { MetricsConfig.None })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertSame(MetricsConfig.None, context.getBean(MetricsConfig::class.java))
            }
    }
}
