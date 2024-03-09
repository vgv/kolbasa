package kolbasa.stats.prometheus.queuesize

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

internal object QueueSizeCache {

    private val locks = ConcurrentHashMap<String, Lock>()
    private val caches: ConcurrentMap<String, CachedValue> = ConcurrentHashMap()

    fun get(queueName: String, maxInterval: Duration, valueCalculationFunc: () -> Long): Long {
        val cachedValue = caches[queueName]
        if (cachedValue != null && cachedValue.isStillValid(maxInterval)) {
            return cachedValue.value
        }

        // Ok, it's time to renew the cache
        val lock = locks.computeIfAbsent(queueName) {
            ReentrantLock()
        }

        return if (lock.tryLock()) {
            try {
                val realValue = valueCalculationFunc()
                caches[queueName] = CachedValue(realValue)
                realValue
            } finally {
                lock.unlock()
            }
        } else {
            // We don't want to wait for the lock, because it affects the performance
            // So, if we have a cached value (even if it's outdated) - return it
            // If we don't have a cached value - return special value, but it's quite rare case
            cachedValue?.value ?: Const.TABLE_SIZE_UNKNOWN_LOCK_CONFLICT
        }
    }
}
