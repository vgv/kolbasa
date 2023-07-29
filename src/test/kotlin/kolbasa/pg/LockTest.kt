package kolbasa.pg

import kolbasa.AbstractPostgresTest
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals

class LockTest : AbstractPostgresTest() {

    @Test
    fun testTryRunExclusive() {
        val lockId = 1313123L
        val threads = 5

        val firstLatch = CountDownLatch(1)
        val secondLatch = CountDownLatch(threads)
        val allThreads = mutableListOf<Thread>()
        val acquiredLocks = CopyOnWriteArrayList<Long?>()

        // Acquire lock in the first thread
        allThreads += thread {
            acquiredLocks += Lock.tryRunExclusive(dataSource, lockId) {
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
                acquiredLocks += Lock.tryRunExclusive(dataSource, lockId) {
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

    @Test
    fun testRawPgLocks() {
        val lockId = 1313123L
        val threads = 5

        val firstLatch = CountDownLatch(1)
        val secondLatch = CountDownLatch(threads)
        val allThreads = mutableListOf<Thread>()
        val acquiredLocks = CopyOnWriteArrayList<Long?>()

        // Acquire lock in the first thread
        allThreads += thread {
            acquiredLocks += Lock.Support.pgTryRunExclusive(dataSource, lockId) {
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
                acquiredLocks += Lock.Support.pgTryRunExclusive(dataSource, lockId) {
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

    @Test
    fun testRawJvmLocks() {
        val lockId = 1313123L
        val threads = 5

        val firstLatch = CountDownLatch(1)
        val secondLatch = CountDownLatch(threads)
        val allThreads = mutableListOf<Thread>()
        val acquiredLocks = CopyOnWriteArrayList<Long?>()

        // Acquire lock in the first thread
        allThreads += thread {
            acquiredLocks += Lock.Support.jvmTryRunExclusive(lockId) {
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
                acquiredLocks += Lock.Support.jvmTryRunExclusive(lockId) {
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
