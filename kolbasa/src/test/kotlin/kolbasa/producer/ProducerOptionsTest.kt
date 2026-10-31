package kolbasa.producer

import io.mockk.mockk
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterProducer
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.datasource.DatabaseProducer
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class ProducerOptionsTest {

    // Never called: the producers only keep them
    private val dataSource = mockk<DataSource>()
    private val cluster = mockk<Cluster>()

    @Test
    fun testOptions() {
        val options = ProducerOptions(producer = "test")

        assertSame(options, ConnectionAwareDatabaseProducer(options).options)
        assertSame(options, DatabaseProducer(dataSource, options).options)
        assertSame(options, ClusterProducer(cluster, options).options)
    }

    @Test
    fun testOptions_Default() {
        assertSame(ProducerOptions.DEFAULT, ConnectionAwareDatabaseProducer().options)
        assertSame(ProducerOptions.DEFAULT, DatabaseProducer(dataSource).options)
        assertSame(ProducerOptions.DEFAULT, ClusterProducer(cluster).options)
    }
}
