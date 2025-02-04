package kolbasa.producer

import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.producer.datasource.Producer
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.test.Test
import kotlin.test.assertEquals

class DataSourceVsConnectionAwareProducersTest {

    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareProducer::class.declaredMemberFunctions
        val producerMethods = Producer::class.declaredMemberFunctions

        // We have twice more methods in Producer than in ConnectionAwareProducer because of sendAsync() methods
        assertEquals(producerMethods.size, connAwareMethods.size * 2)
    }

}
