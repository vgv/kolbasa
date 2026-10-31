package kolbasa.consumer

import kolbasa.assertSameMethods
import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.consumer.datasource.Consumer
import org.junit.jupiter.api.Test

class DataSourceVsConnectionAwareConsumersTest {

    @Test
    fun testSameMethods() {
        assertSameMethods(Consumer::class.java, ConnectionAwareConsumer::class.java)
    }
}
