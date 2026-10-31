package kolbasa.consumer

import io.mockk.mockk
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterConsumer
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.datasource.DatabaseConsumer
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class ConsumerOptionsTest {

    // Never called: the consumers only keep them
    private val dataSource = mockk<DataSource>()
    private val cluster = mockk<Cluster>()

    @Test
    fun testOptions() {
        val options = ConsumerOptions(consumer = "test")

        assertSame(options, ConnectionAwareDatabaseConsumer(options).options)
        assertSame(options, DatabaseConsumer(dataSource, options).options)
        assertSame(options, ClusterConsumer(cluster, options).options)
    }

    @Test
    fun testOptions_Default() {
        assertSame(ConsumerOptions.DEFAULT, ConnectionAwareDatabaseConsumer().options)
        assertSame(ConsumerOptions.DEFAULT, DatabaseConsumer(dataSource).options)
        assertSame(ConsumerOptions.DEFAULT, ClusterConsumer(cluster).options)
    }
}
