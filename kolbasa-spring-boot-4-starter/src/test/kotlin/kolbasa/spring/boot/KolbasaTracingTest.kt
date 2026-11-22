package kolbasa.spring.boot

import io.opentelemetry.api.OpenTelemetry
import kolbasa.opentelemetry.OpenTelemetryTracing
import kolbasa.telemetry.tracing.Tracing
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.opentelemetry.autoconfigure.OpenTelemetrySdkAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class KolbasaTracingTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))

    @Test
    fun testAfterName_RealClass() {
        // Spring Boot skips a name it can't find, so a typo would quietly break the order
        assertEquals(OpenTelemetrySdkAutoConfiguration::class.java.name, OPEN_TELEMETRY_SDK_AUTO_CONFIGURATION)
    }

    @Test
    fun testSpringBootOpenTelemetry_TracingOverIt() {
        // Spring Boot makes the OpenTelemetry bean in its own auto-configuration, so kolbasa's must run after it
        contextRunner
            .withConfiguration(AutoConfigurations.of(OpenTelemetrySdkAutoConfiguration::class.java))
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context).hasSingleBean(OpenTelemetry::class.java)
                assertTrue(context.getBean(Tracing::class.java) is OpenTelemetryTracing)
            }
    }

    @Test
    fun testOwnOpenTelemetry_TracingOverIt() {
        contextRunner
            .withBean(OpenTelemetry::class.java, { OpenTelemetry.noop() })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertTrue(context.getBean(Tracing::class.java) is OpenTelemetryTracing)
            }
    }

    @Test
    fun testNoOpenTelemetry_NoTracing() {
        contextRunner.run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context).doesNotHaveBean(Tracing::class.java)
        }
    }

    @Test
    fun testOwnTracing_ReplacesStarters() {
        contextRunner
            .withBean(OpenTelemetry::class.java, { OpenTelemetry.noop() })
            .withBean(Tracing::class.java, { Tracing.NONE })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertSame(Tracing.NONE, context.getBean(Tracing::class.java))
            }
    }
}
