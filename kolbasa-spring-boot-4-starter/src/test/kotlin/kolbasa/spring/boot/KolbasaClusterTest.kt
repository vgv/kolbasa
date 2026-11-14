package kolbasa.spring.boot

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import kolbasa.cluster.*
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.inspector.datasource.DatabaseInspector
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.ProducerOptions
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.telemetry.metrics.MetricsConfig
import kolbasa.test.AbstractPostgreSQLTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.util.concurrent.ScheduledThreadPoolExecutor
import javax.sql.DataSource

class KolbasaClusterTest : AbstractPostgreSQLTest() {

    private val nodes by lazy { listOf(dataSourceFirstSchema, dataSourceSecondSchema) }

    // No DataSource bean: a cluster doesn't need the application's own database
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))

    @Test
    fun testClusterSettings_ClusterReady() {
        contextRunner
            .withBean(KolbasaClusterSettings::class.java, { KolbasaClusterSettings(nodes, listOf(QUEUE)) })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertTrue("q_test" in tables(dataSourceFirstSchema, FIRST_SCHEMA_NAME))
                assertTrue("q_test" in tables(dataSourceSecondSchema, SECOND_SCHEMA_NAME))

                val cluster = context.getBean(Cluster::class.java)
                ClusterProducer(cluster).send(QUEUE, "hello").throwExceptionIfAny()
            }
    }

    @Test
    fun testFirstTopologyReadFails_StartupFails() {
        contextRunner
            .withBean(KolbasaClusterSettings::class.java, { KolbasaClusterSettings(emptyList(), listOf(QUEUE)) })
            .run { context ->
                assertThat(context).hasFailed()
                val rootCause = generateSequence(context.startupFailure) { it.cause }.last()
                assertTrue("Data sources list is empty" in rootCause.message.orEmpty())
            }
    }

    @Test
    fun testClusterSettings_RolesGetOptions() {
        // The updater schedules its next read here, so the test sees that it got the options
        val executor = ScheduledThreadPoolExecutor(1)
        val settings = KolbasaClusterSettings.builder(nodes, listOf(QUEUE))
            .producerOptions(ProducerOptions(producer = "test"))
            .consumerOptions(ConsumerOptions(consumer = "test"))
            .mutatorOptions(MutatorOptions(metricsConfig = MetricsConfig.None))
            .shardStrategy(ShardStrategy.Fixed(SHARD))
            .topologyUpdaterOptions(ClusterTopologyUpdaterOptions(executor = executor))
            .build()

        try {
            contextRunner
                .withBean(KolbasaClusterSettings::class.java, { settings })
                .run { context ->
                    assertThat(context).hasNotFailed()
                    assertEquals(settings.consumerOptions, context.getBean(Consumer::class.java).options)
                    assertEquals(settings.mutatorOptions, context.getBean(Mutator::class.java).options)
                    assertThat(context).hasSingleBean(Inspector::class.java)
                    assertEquals(1, executor.queue.size)

                    val producer = context.getBean(Producer::class.java)
                    assertEquals(settings.producerOptions, producer.options)
                    val sent = producer.send(QUEUE, "hello").onlySuccessful().single()
                    assertEquals(SHARD, sent.id.shard.id)
                }
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun testMeterRegistry_RolesReportToIt() {
        val registry = SimpleMeterRegistry()

        contextRunner
            .withBean(KolbasaClusterSettings::class.java, { KolbasaClusterSettings(nodes, listOf(QUEUE)) })
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
    fun testRoleUsedWhileStarting_FindsClusterReady() {
        contextRunner
            .withBean(KolbasaClusterSettings::class.java, { KolbasaClusterSettings(nodes, listOf(QUEUE)) })
            .withBean(StartupSender::class.java)
            .run { context ->
                assertThat(context).hasNotFailed()
            }
    }

    @Test
    fun testOwnDatabaseRoles_ClusterRolesStay() {
        // Beans of yours that work with one database don't switch the cluster's roles off
        contextRunner
            .withBean(KolbasaClusterSettings::class.java, { KolbasaClusterSettings(nodes, listOf(QUEUE)) })
            .withBean(DatabaseProducer::class.java, { mockk() })
            .withBean(DatabaseConsumer::class.java, { mockk() })
            .withBean(DatabaseMutator::class.java, { mockk() })
            .withBean(DatabaseInspector::class.java, { mockk() })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context).hasBean("kolbasaClusterProducer")
                assertThat(context).hasBean("kolbasaClusterConsumer")
                assertThat(context).hasBean("kolbasaClusterMutator")
                assertThat(context).hasBean("kolbasaClusterInspector")
            }
    }

    @Test
    fun testStandaloneAndCluster_BothRoleSets() {
        contextRunner
            .withBean(DataSource::class.java, { dataSource })
            .withBean(KolbasaStandaloneSettings::class.java, { KolbasaStandaloneSettings(listOf(QUEUE)) })
            .withBean(KolbasaClusterSettings::class.java, { KolbasaClusterSettings(nodes, listOf(QUEUE)) })
            .withBean(BothRoleSets::class.java)
            .run { context ->
                assertThat(context).hasNotFailed()

                val roles = context.getBean(BothRoleSets::class.java)
                assertTrue(roles.standaloneProducer is DatabaseProducer)
                assertTrue(roles.standaloneConsumer is DatabaseConsumer)
                assertTrue(roles.standaloneMutator is DatabaseMutator)
                assertTrue(roles.standaloneInspector is DatabaseInspector)
                assertTrue(roles.clusterProducer is ClusterProducer)
                assertTrue(roles.clusterConsumer is ClusterConsumer)
                assertTrue(roles.clusterMutator is ClusterMutator)
                assertTrue(roles.clusterInspector is ClusterInspector)
            }
    }

    // Sends a message while it is created. Spring creates it before the starter's beans, so the send works only if
    // the Producer brings the topology with it
    class StartupSender(producer: Producer) {
        init {
            producer.send(QUEUE, "hello").throwExceptionIfAny()
        }
    }

    // Gets every role twice, the way an application with both settings beans does
    class BothRoleSets(
        @KolbasaStandalone val standaloneProducer: Producer,
        @KolbasaStandalone val standaloneConsumer: Consumer,
        @KolbasaStandalone val standaloneMutator: Mutator,
        @KolbasaStandalone val standaloneInspector: Inspector,
        @KolbasaCluster val clusterProducer: Producer,
        @KolbasaCluster val clusterConsumer: Consumer,
        @KolbasaCluster val clusterMutator: Mutator,
        @KolbasaCluster val clusterInspector: Inspector
    )

    private fun tables(dataSource: DataSource, schema: String): Set<String> = dataSource.connection.use { connection ->
        connection.prepareStatement("select table_name from information_schema.tables where table_schema = ?").use {
            it.setString(1, schema)
            it.executeQuery().use { resultSet ->
                buildSet { while (resultSet.next()) add(resultSet.getString(1)) }
            }
        }
    }

    private companion object {
        const val SHARD = 7
        val QUEUE = Queue.of("test", PredefinedDataTypes.String)
    }
}
