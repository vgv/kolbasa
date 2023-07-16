package kolbasa.task

import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CleanupConfigTest {

    @Test
    fun getDefaultCleanupLockIdGenerator() {
        val cleanupConfig = CleanupConfig()
        val testQueue = Queue<String, Unit>("test_queue", PredefinedDataTypes.String)

        assertEquals(724226541, cleanupConfig.cleanupLockIdGenerator(testQueue))
    }

}
