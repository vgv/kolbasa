package kolbasa

import kolbasa.producer.SendMessage
import kolbasa.producer.SendRequest
import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.queue.Queue
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.sql.Connection

class KotlinCompilationSettingsTest {

    @Test
    fun testJvmDefaultIntefaceMethods() {
        // We need to generate JVM default methods for interfaces, but Kotlin compiler has different options
        // Let's check that default methods are generated
        // More info: https://kotlinlang.org/docs/interfaces.html#jvm-default-method-generation-for-interface-functions
        //

        // Should be default method, because this method has implementation in interface
        val firstSendMethod = ConnectionAwareProducer::class.java.getMethod(
            "send",
            Connection::class.java,
            Queue::class.java,
            SendMessage::class.java
        )
        assertTrue(firstSendMethod.isDefault, "Method: $firstSendMethod")

        // Shouldn't be default method, because this method doesn't have implementation in interface
        val secondSendMethod = ConnectionAwareProducer::class.java.getMethod(
            "send",
            Connection::class.java,
            Queue::class.java,
            SendRequest::class.java
        )
        assertFalse(secondSendMethod.isDefault, "Method: $firstSendMethod")
    }

}
