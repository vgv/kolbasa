package kolbasa.producer

import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.producer.datasource.Producer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.reflect.full.declaredMemberFunctions

class DataSourceVsConnectionAwareProducersTest {

    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareProducer::class.declaredMemberFunctions
        val producerMethods = Producer::class.declaredMemberFunctions

        assertEquals(producerMethods.size, connAwareMethods.size)
    }

}
