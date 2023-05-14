package kolbasa.producer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.memberFunctions

class ProducersTest {

    @Test
    fun testSameMethods() {
        val connAwareMethods = ConnectionAwareProducer::class.declaredMemberFunctions
        val producerMethods = Producer::class.declaredMemberFunctions

        assertEquals(producerMethods.size, connAwareMethods.size)
    }

}
