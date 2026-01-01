package kolbasa.consumer

import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.consumer.datasource.Consumer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.reflect.full.declaredMemberFunctions

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
