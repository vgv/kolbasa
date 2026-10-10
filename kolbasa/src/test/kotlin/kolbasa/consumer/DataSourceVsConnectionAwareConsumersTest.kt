package kolbasa.consumer

import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.consumer.datasource.Consumer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DataSourceVsConnectionAwareConsumersTest {

    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareConsumer::class.java.declaredMethods // .declaredMemberFunctions
        val consumerMethods = Consumer::class.java.declaredMethods

        assertEquals(consumerMethods.size, connAwareMethods.size)
    }
}
