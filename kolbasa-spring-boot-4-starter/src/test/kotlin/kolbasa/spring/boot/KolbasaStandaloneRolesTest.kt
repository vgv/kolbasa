package kolbasa.spring.boot

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.mockk.mockk
import kolbasa.cluster.ClusterConsumer
import kolbasa.cluster.ClusterInspector
import kolbasa.cluster.ClusterMutator
import kolbasa.cluster.ClusterProducer
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.datasource.Consumer
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.ProducerOptions
import kolbasa.producer.datasource.Producer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.telemetry.metrics.MetricsConfig
import kolbasa.telemetry.tracing.Tracing
import kolbasa.test.AbstractPostgreSQLTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.opentelemetry.autoconfigure.OpenTelemetrySdkAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import javax.sql.DataSource

class KolbasaStandaloneRolesTest : AbstractPostgreSQLTest() {

    private val settings = KolbasaStandaloneSettings(listOf(QUEUE))

    // The context closes the DataSource when it stops, so every check runs inside run {}
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))
        .withBean(DataSource::class.java, { dataSource })

    @Test
    fun testStandaloneSettings_RolesGetOptions() {
        val settings = KolbasaStandaloneSettings.builder(listOf(QUEUE))
            .producerOptions(ProducerOptions(producer = "test"))
            .consumerOptions(ConsumerOptions(consumer = "test"))
            .mutatorOptions(MutatorOptions(metricsConfig = MetricsConfig.None))
            .build()

        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertEquals(settings.producerOptions, context.getBean(Producer::class.java).options)
                assertEquals(settings.consumerOptions, context.getBean(Consumer::class.java).options)
                assertEquals(settings.mutatorOptions, context.getBean(Mutator::class.java).options)
                assertThat(context).hasSingleBean(Inspector::class.java)
            }
    }

    @Test
    fun testMeterRegistry_RolesReportToIt() {
        val registry = SimpleMeterRegistry()

        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withBean(MeterRegistry::class.java, { registry })
            .run { context ->
                assertThat(context).hasNotFailed()
                val metricsConfig = MetricsConfig.Config(registry)
                assertEquals(metricsConfig, context.getBean(Producer::class.java).options.metricsConfig)
                assertEquals(metricsConfig, context.getBean(Consumer::class.java).options.metricsConfig)
                assertEquals(metricsConfig, context.getBean(Mutator::class.java).options.metricsConfig)

                context.getBean(Producer::class.java).send(QUEUE, "hello").throwExceptionIfAny()
                // One counter for every partial insert type, so the message is in one of them
                val sent = registry.find("kolbasa.producer.send.messages").counters().sumOf { it.count() }
                assertEquals(1.0, sent)
            }
    }

    @Test
    fun testMetricsConfigInOptions_Stays() {
        // Options with their own metrics config keep it, the others get the registry
        val settings = KolbasaStandaloneSettings.builder(listOf(QUEUE))
            .producerOptions(ProducerOptions(metricsConfig = MetricsConfig.None))
            .build()
        val registry = SimpleMeterRegistry()

        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withBean(MeterRegistry::class.java, { registry })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertSame(MetricsConfig.None, context.getBean(Producer::class.java).options.metricsConfig)
                val consumer = context.getBean(Consumer::class.java)
                assertEquals(MetricsConfig.Config(registry), consumer.options.metricsConfig)
            }
    }

    @Test
    fun testSpringBootOpenTelemetry_ProducerAndConsumerTrace() {
        // Spring Boot's own OpenTelemetry bean, over a tracer provider that records the spans
        val spans = RecordingSpanProcessor()

        contextRunner
            .withConfiguration(AutoConfigurations.of(OpenTelemetrySdkAutoConfiguration::class.java))
            .withBean(SdkTracerProvider::class.java, { SdkTracerProvider.builder().addSpanProcessor(spans).build() })
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .run { context ->
                assertThat(context).hasNotFailed()
                context.getBean(Producer::class.java).send(QUEUE, "hello").throwExceptionIfAny()
                assertNotNull(context.getBean(Consumer::class.java).receive(QUEUE))
                assertEquals(listOf("test publish", "test receive"), spans.ended)
            }
    }

    @Test
    fun testTracingInOptions_Stays() {
        // Options with their own tracing keep it, the others get the Tracing bean
        val settings = KolbasaStandaloneSettings.builder(listOf(QUEUE))
            .producerOptions(ProducerOptions(tracing = Tracing.NONE))
            .build()

        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withBean(OpenTelemetry::class.java, { OpenTelemetry.noop() })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertSame(Tracing.NONE, context.getBean(Producer::class.java).options.tracing)
                val consumer = context.getBean(Consumer::class.java)
                assertSame(context.getBean(Tracing::class.java), consumer.options.tracing)
            }
    }

    @Test
    fun testOwnProducer_ReplacesStarters() {
        val producer = mockk<Producer>()

        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withBean(Producer::class.java, { producer })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertSame(producer, context.getBean(Producer::class.java))
            }
    }

    @Test
    fun testOwnClusterRoles_StandaloneRolesStay() {
        // Cluster beans of yours don't switch the standalone roles off
        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withBean(ClusterProducer::class.java, { mockk() })
            .withBean(ClusterConsumer::class.java, { mockk() })
            .withBean(ClusterMutator::class.java, { mockk() })
            .withBean(ClusterInspector::class.java, { mockk() })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context).hasBean("kolbasaStandaloneProducer")
                assertThat(context).hasBean("kolbasaStandaloneConsumer")
                assertThat(context).hasBean("kolbasaStandaloneMutator")
                assertThat(context).hasBean("kolbasaStandaloneInspector")
            }
    }

    @Test
    fun testRoleUsedWhileStarting_FindsTables() {
        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withBean(StartupSender::class.java)
            .run { context ->
                assertThat(context).hasNotFailed()
            }
    }

    // Sends a message while it is created. Spring creates it before the starter's beans, so the send works only if
    // the Producer brings the tables with it
    class StartupSender(producer: Producer) {
        init {
            producer.send(QUEUE, "hello").throwExceptionIfAny()
        }
    }

    private companion object {
        val QUEUE = Queue.of("test", PredefinedDataTypes.String)
    }
}
