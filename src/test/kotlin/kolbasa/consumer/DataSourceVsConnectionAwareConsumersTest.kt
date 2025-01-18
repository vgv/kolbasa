package kolbasa.consumer

import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.consumer.datasource.Consumer
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.test.Test
import kotlin.test.assertEquals

class DataSourceVsConnectionAwareConsumersTest {

    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareConsumer::class.declaredMemberFunctions
        val consumerMethods = Consumer::class.declaredMemberFunctions

        assertEquals(consumerMethods.size, connAwareMethods.size)
    }
}
