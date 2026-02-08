package kolbasa.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.fail
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.intArrayOf

class LruCacheTest {

    @Test
    fun testCornerCases() {
        // Capacity
        assertThrows<IllegalArgumentException> { LruCache<String, String>(0) }.also {
            assertEquals("capacity must be positive, but was: 0", it.message)
        }

        assertThrows<IllegalArgumentException> { LruCache<String, String>(-1) }.also {
            assertEquals("capacity must be positive, but was: -1", it.message)
        }

        // evictionInterval
        assertThrows<IllegalArgumentException> { LruCache<String, String>(100, evictionInterval = 0) }.also {
            assertEquals("evictionInterval must be at least 1, but was: 0", it.message)
        }

        // entriesRemovedAtEviction
        assertThrows<IllegalArgumentException> { LruCache<String, String>(100, entriesRemovedAtEviction = 0) }.also {
            assertEquals("entriesRemovedAtEviction must be in range [1..100], but was: 0" , it.message)
        }
        assertThrows<IllegalArgumentException> { LruCache<String, String>(100, entriesRemovedAtEviction = 101) }.also {
            assertEquals("entriesRemovedAtEviction must be in range [1..100], but was: 101" , it.message)
        }
    }

    @Test
    fun testBasicGetPut() {
        val cache = LruCache<String, Int>(10)

        assertNull(cache.get("key1"))
        assertEquals(0, cache.size())

        assertNull(cache.put("key1", 100))
        assertEquals(100, cache.get("key1"))
        assertEquals(1, cache.size())

        // Updating existing key
        assertEquals(100, cache.put("key1", 200))
        assertEquals(200, cache.get("key1"))
        assertEquals(1, cache.size())
    }

    @Test
    fun testGetOrPut() {
        val cache = LruCache<String, Int>(10)
        var computeCount = 0

        val value1 = cache.getOrPut("key1") {
            computeCount++
            100
        }
        assertEquals(100, value1)
        assertEquals(1, computeCount)

        // Second call should not compute
        val value2 = cache.getOrPut("key1") {
            computeCount++
            200
        }
        assertEquals(100, value2)
        assertEquals(1, computeCount)
    }

    @Test
    fun testGetUpdatesAccessOrder() {
        val cache = LruCache<String, Int>(3, evictionInterval = 1)

        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)

        // Access 'a' to make it most recently used
        cache.get("a")

        // Add new entry to trigger eviction
        cache.put("d", 4)

        // 'a' should survive, 'b' should be evicted (oldest)
        assertEquals(1, cache.get("a"))
        assertNull(cache.get("b"))
    }

    @Test
    fun testGetOrPutUpdatesAccessOrder() {
        val cache = LruCache<String, Int>(3)

        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)

        // Access 'a' via getOrPut to make it most recently used
        cache.getOrPut("a") { 999 }

        // Add new entry to trigger eviction
        cache.put("d", 4)

        // 'a' should survive, 'b' should be evicted (oldest)
        assertEquals(1, cache.get("a"))
        assertNull(cache.get("b"))
    }

    @Test
    fun testClear() {
        val cache = LruCache<String, Int>(10)

        cache.put("key1", 100)
        cache.put("key2", 200)
        assertEquals(2, cache.size())

        cache.clear()
        assertEquals(0, cache.size())
        assertNull(cache.get("key1"))
        assertNull(cache.get("key2"))
    }

    @Test
    fun testLruEviction() {
        val cache = LruCache<String, Int>(5, evictionInterval = 1, entriesRemovedAtEviction = 1)

        // Fill the cache
        cache.put("key1", 1)
        cache.put("key2", 2)
        cache.put("key3", 3)
        cache.put("key4", 4)
        cache.put("key5", 5)

        assertEquals(5, cache.size())

        // Access key1 to make it recently used
        cache.get("key1")

        // Add a new entry to trigger eviction
        cache.put("key6", 6)

        // key2 should be evicted (least recently used)
        assertNull(cache.get("key2"))

        // key1 and key6 should still be present
        assertEquals(1, cache.get("key1"))
        assertEquals(6, cache.get("key6"))
        assertEquals(5, cache.size())
    }

    @ParameterizedTest
    @ValueSource(ints = [5, 15, 65])
    fun testBulkEviction(removeDuringEviction: Int) {
        val capacity = 100
        val cache = LruCache<String, Int>(capacity, evictionInterval = 1, entriesRemovedAtEviction = removeDuringEviction)

        // Fill the cache
        for (i in 1..100) {
            cache.put("key$i", i)
        }
        assertEquals(100, cache.size())

        // Add one more entry to trigger eviction
        cache.put("key101", 101)

        // Should have evicted the oldest entries
        assertEquals((capacity - removeDuringEviction) + 1, cache.size())
        (1..removeDuringEviction).forEach {
            assertNull(cache.get("key$it"))
        }

        assertEquals(101, cache.get("key101"))
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 7, 13])
    fun testConcurrentAccess(threadsToCreate: Int) {
        val cacheMaxCapacity = 1000
        val cache = LruCache<Int, Int>(cacheMaxCapacity)
        val operationsPerThread = cacheMaxCapacity * 100
        val startLatch = CountDownLatch(1)
        val exception = AtomicReference<Throwable>()

        val threads = (1..threadsToCreate).map { threadIndex ->
            thread {
                startLatch.await()
                try {
                    (1..operationsPerThread).forEach { i ->
                        val key = threadIndex * operationsPerThread + i
                        cache.put(key, key)
                        cache.get(key)
                        cache.getOrPut(key) { key * 2 }
                    }
                } catch (e: Throwable) {
                    exception.set(e)
                    throw e
                }
            }
        }

        startLatch.countDown()
        threads.forEach { it.join() }

        // Check for exceptions from threads
        if (exception.get() != null) {
            fail(exception.get())
        }

        // Cache size is bounded, but can temporarily exceed capacity between eviction checks
        // Usually it should not exceed capacity + (capacity / 10) due to eviction strategy, but let's make it more lenient here
        // and just check it isn't exceeding capacity * 50%
        assertTrue(
            cache.size() <= cacheMaxCapacity * 1.1,
            "Max capacity: $cacheMaxCapacity, cache size: ${cache.size()}"
        )
    }

    @Test
    fun testCacheStats() {
        val capacity = 1000
        val cache = LruCache<String, String>(capacity)

        // before any operation
        cache.stats().also { stats ->
            assertEquals(capacity, stats.capacity)
            assertEquals(0L, stats.hits)
            assertEquals(0L, stats.misses)
            assertEquals(0L, stats.puts)
            assertEquals(0L, stats.evictions)
        }

        (1..10).forEach {
            cache.put("key_$it", "value_$it")
        }

        // after 10 puts
        cache.stats().also { stats ->
            assertEquals(capacity, stats.capacity)
            assertEquals(0L, stats.hits)
            assertEquals(0L, stats.misses)
            assertEquals(10L, stats.puts)
            assertEquals(0L, stats.evictions)
        }

        // 10 hits, 3 misses
        (1..13).forEach {
            cache.get("key_$it")
        }

        // after 10 hits and 3 misses
        cache.stats().also { stats ->
            assertEquals(capacity, stats.capacity)
            assertEquals(10L, stats.hits)
            assertEquals(3L, stats.misses)
            assertEquals(10L, stats.puts)
            assertEquals(0L, stats.evictions)
        }

        // trigger some evictions
        (1..capacity * 2).forEach {
            cache.put("key_$it", "value_$it")
        }

        // after evictions hits and 3 misses
        cache.stats().also { stats ->
            assertEquals(capacity, stats.capacity)
            assertEquals(10L, stats.hits)
            assertEquals(3L, stats.misses)
            assertEquals(10L + capacity * 2, stats.puts)
            assertEquals(capacity.toLong(), stats.evictions)
        }
    }

}
