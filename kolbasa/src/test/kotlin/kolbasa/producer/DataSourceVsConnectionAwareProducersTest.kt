package kolbasa.producer

import kolbasa.assertSameMethods
import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.producer.datasource.Producer
import org.junit.jupiter.api.Test

class DataSourceVsConnectionAwareProducersTest {

    @Test
    fun testSameMethods() {
        assertSameMethods(Producer::class.java, ConnectionAwareProducer::class.java)
    }
}
