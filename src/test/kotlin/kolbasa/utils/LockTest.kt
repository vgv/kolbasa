package kolbasa.utils

import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals

class LockTest {

    @Test
    fun testTryRunExclusive() {
        val lockName = "bugaga_test_lock_name"
        val threads = 5

        val firstLatch = CountDownLatch(1)
        val secondLatch = CountDownLatch(threads)
        val allThreads = mutableListOf<Thread>()
        val acquiredLocks = CopyOnWriteArrayList<Long?>()

        // Acquire lock in the first thread
        allThreads += thread {
            acquiredLocks += Lock.tryRunExclusive(lockName) {
                // start other threads
                firstLatch.countDown()

                // wait other threads finish
                secondLatch.await()

                // Return ID of the thread that acquired the lock
                Thread.currentThread().id
            }
        }

        // Try to acquire the same lock in other threads
        (1..threads).forEach { _ ->
            allThreads += thread {
                // Wait until first thread acquires the lock
                firstLatch.await()

                // Try to acquire the same lock in this thread
                acquiredLocks += Lock.tryRunExclusive(lockName) {
                    // Return ID of the thread that acquired the lock
                    Thread.currentThread().id
                }

                // Signal this thread is over
                secondLatch.countDown()
            }
        }

        // Wait for all threads
        allThreads.forEach(Thread::join)

        // Test the lock was acquired by the first thread
        assertEquals(allThreads.first().id, acquiredLocks.filterNotNull().first())
        // Test all required threads were launched
        assertEquals(threads + 1, allThreads.size)
        // Test no other locks were acquired
        assertEquals(1, acquiredLocks.filterNotNull().size, "Results: $acquiredLocks")
    }

}
