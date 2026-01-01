package kolbasa.utils

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DaemonThreadFactoryTest {

    @Test
    fun testNewThread() {
        // Two different factories create threads independently
        DaemonThreadFactory("first_factory").newThread(mockk()).also { thread ->
            assertTrue(thread.isDaemon)
            assertEquals("first_factory-1", thread.name)
        }

        DaemonThreadFactory("second_factory").newThread(mockk()).also { thread ->
            assertTrue(thread.isDaemon)
            assertEquals("second_factory-2", thread.name)
        }
    }

}
