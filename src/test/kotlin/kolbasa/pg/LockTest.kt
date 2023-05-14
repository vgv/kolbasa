package kolbasa.pg

import kolbasa.AbstractPostgresTest
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread
import kotlin.test.assertTrue

class LockTest : AbstractPostgresTest() {

    @Test
    fun testTryRunExclusive() {
        val lockId = 1313123L

        // Lock
        thread {
            Lock.tryRunExclusive(dataSource, lockId) {
                Thread.sleep(1_000)
            }
        }

        Thread.sleep(50)

        // Try to lock the same lock
        val results = CopyOnWriteArrayList<Int>()
        val threads = (1..5).map { threadId ->
            thread {
                Lock.tryRunExclusive(dataSource, lockId) {
                    results += threadId
                }
            }
        }
        threads.forEach(Thread::join)

        // Test no locks were acquired
        assertTrue(results.isEmpty(), "Results: $results")
    }
}
