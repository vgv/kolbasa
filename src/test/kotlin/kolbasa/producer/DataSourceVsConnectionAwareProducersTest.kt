package kolbasa.producer

import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.producer.datasource.Producer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DataSourceVsConnectionAwareProducersTest {

    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareProducer::class.java.declaredMethods
        val producerMethods = Producer::class.java.declaredMethods

        // We have twice more methods in Producer than in ConnectionAwareProducer because of sendAsync() methods
        assertEquals(producerMethods.size, connAwareMethods.size * 2)
    }

}
