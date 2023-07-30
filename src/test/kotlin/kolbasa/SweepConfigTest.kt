package kolbasa

import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SweepConfigTest {

    @Test
    fun getDefaultSweepLockIdGenerator() {
        val sweepConfig = SweepConfig()
        val testQueue = Queue<String, Unit>("test_queue", PredefinedDataTypes.String)

        assertEquals(1424981157, sweepConfig.lockIdGenerator(testQueue))
    }

}
