package kolbasa.spring.boot

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
import kolbasa.test.AbstractPostgreSQLTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
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
