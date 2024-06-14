package kolbasa.utils

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

internal object Lock {

    fun <R> tryRunExclusive(lockName: String, block: () -> R): R? {
        val lock = jvmLocks.computeIfAbsent(lockName) { _ ->
            ReentrantLock()
        }

        return if (lock.tryLock()) {
            try {
                block()
            } finally {
                lock.unlock()
            }
        } else {
            null
        }
    }

    private val jvmLocks = ConcurrentHashMap<String, Lock>()

}
