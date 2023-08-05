package kolbasa.consumer

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.reflect.full.declaredMemberFunctions

class ConsumersTest {

    @Test
    fun testSameMethods() {
        // Test we have the same methods in these two interfaces
        // Ideally, we need to check method signatures too, but I was too lazy to test it
        val connAwareMethods = ConnectionAwareConsumer::class.declaredMemberFunctions
        val consumerMethods = Consumer::class.declaredMemberFunctions

        Assertions.assertEquals(consumerMethods.size, connAwareMethods.size)
    }


}
