package kolbasa.utils

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.max

internal class LruCache<K : Any, V>(
    private val capacity: Int,

    /**
     * Start eviction before (or after) every put operation isn't very efficient, so we only check for eviction every Nth write.
     *
     * By default, this interval is 5% of capacity writes, e.g. if capacity=1000, then we check every 50th put operation should
     * we start eviction or not.
     */
    private val evictionInterval: Int = maxOf(1, capacity / 20),

    /**
     * Removing only one value from the cache during eviction isn't very efficient, so we find and remove old values in batches.
     * This parameter controls how many entries are removed at once during eviction.
     *
     * By default, we remove 10% of capacity entries at once, e.g. if capacity=1000, then we remove 100 entries at every eviction.
     */
    private val entriesRemovedAtEviction: Int = maxOf(1, capacity / 10),
) {

    init {
        require(capacity > 0) { "capacity must be positive, but was: $capacity" }
        require(evictionInterval >= 1) { "evictionInterval must be at least 1, but was: $evictionInterval" }
        require(entriesRemovedAtEviction in 1..capacity) { "entriesRemovedAtEviction must be in range [1..$capacity], but was: $entriesRemovedAtEviction" }
    }

    private val cache = ConcurrentHashMap<K, CacheEntry<V>>(capacity)
    private val accessCounter = AtomicLong(0)
    private val writeCounter = AtomicInteger(0)
    private val evictionLock = ReentrantLock()

    // Stats
    private val hits = LongAdder()
    private val misses = LongAdder()
    private val puts = LongAdder()
    private val evictions = LongAdder()

    private class CacheEntry<V>(
        val value: V,
        @Volatile
        var accessOrder: Long
    )

    fun get(key: K): V? {
        val entry = cache[key]

        return if (entry != null) {
            hits.increment()
            entry.accessOrder = accessCounter.incrementAndGet()
            entry.value
        } else {
            misses.increment()
            null
        }
    }

    fun put(key: K, value: V): V? {
        val oldEntry = cache.put(key, CacheEntry(value, accessCounter.incrementAndGet()))

        puts.increment()

        tryEvict()

        return oldEntry?.value
    }

    fun getOrPut(key: K, defaultValue: () -> V): V {
        // Fast path: entry exists
        val existingEntry = cache[key]
        if (existingEntry != null) {
            hits.increment()
            existingEntry.accessOrder = accessCounter.incrementAndGet()
            return existingEntry.value
        } else {
            misses.increment()
        }

        // Slow path: compute if absent
        val entry = cache.computeIfAbsent(key) {
            puts.increment()
            CacheEntry(defaultValue(), accessCounter.incrementAndGet())
        }

        tryEvict()

        return entry.value
    }

    fun size(): Int = cache.size
    fun clear(): Unit = cache.clear()

    fun stats(): CacheStats = CacheStats(
        capacity = capacity,
        hits = hits.sum(),
        misses = misses.sum(),
        puts = puts.sum(),
        evictions = evictions.sum(),
    )

    private fun tryEvict() {
        // only evict every Nth write (N = 5% of capacity)
        if (writeCounter.incrementAndGet() % evictionInterval != 0) {
            return
        }

        val cacheSize = cache.size
        if (cacheSize <= capacity) {
            return
        }

        if (evictionLock.isLocked) {
            while (evictionLock.isLocked) {
                Thread.onSpinWait()
            }
        } else {
            evictionLock.lock()
            try {
                evict(cacheSize)
            } finally {
                evictionLock.unlock()
            }
        }
    }

    private fun evict(currentCacheSize: Int) {
        // Put all cache records into a sorted map based on access order
        val sortedMap = TreeMap<Long, K>()
        cache.entries.forEach { (key, entry) ->
            sortedMap[entry.accessOrder] = key
        }

        var entriesToRemove = max(entriesRemovedAtEviction, (currentCacheSize - capacity))

        val iterator = sortedMap.values.iterator()
        while (iterator.hasNext() && entriesToRemove-- > 0) {
            cache.remove(iterator.next())
            evictions.increment()
        }
    }
}

internal data class CacheStats(
    val capacity: Int,
    val hits: Long,
    val misses: Long,
    val puts: Long,
    val evictions: Long,
)
