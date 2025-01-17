package kolbasa.producer

import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.producer.datasource.Producer
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals

class DataSourceVsConnectionAwareProducersTest {

    // TODO: enable it again after full migration completed
    @Ignore
    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareProducer::class.declaredMemberFunctions
        val producerMethods = Producer::class.declaredMemberFunctions

        assertEquals(producerMethods.size, connAwareMethods.size)
    }

}
