package kolbasa.spring.boot

import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.test.AbstractPostgreSQLTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import javax.sql.DataSource

class KolbasaClusterTest : AbstractPostgreSQLTest() {

    // No DataSource bean: a cluster doesn't need the application's own database
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))

    @Test
    fun testClusterSettings_ClusterReady() {
        val nodes = listOf(dataSourceFirstSchema, dataSourceSecondSchema)

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

    private fun tables(dataSource: DataSource, schema: String): Set<String> = dataSource.connection.use { connection ->
        connection.prepareStatement("select table_name from information_schema.tables where table_schema = ?").use {
            it.setString(1, schema)
            it.executeQuery().use { resultSet ->
                buildSet { while (resultSet.next()) add(resultSet.getString(1)) }
            }
        }
    }

    private companion object {
        val QUEUE = Queue.of("test", PredefinedDataTypes.String)
    }
}
