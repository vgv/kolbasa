package kolbasa.task

import kolbasa.queue.Checks
import kolbasa.queue.Queue

data class CleanupConfig(
    val enabled: Boolean = true,
    val cleanupLimit: Int = 1000,
    val cleanupMaxIterations: Int = 10,
    val cleanupProbabilityPercent: Int = 20,
    val cleanupLockIdGenerator: (queue: Queue<*, *>) -> Long = ::defaultLockIdGenerator
) {

    init {
        Checks.checkCleanupLimit(cleanupLimit)
        Checks.checkCleanupMaxIterations(cleanupMaxIterations)
        Checks.checkCleanupProbabilityPercent(cleanupProbabilityPercent)
    }

    private companion object {
        fun defaultLockIdGenerator(queue: Queue<*, *>): Long {
            return "cleanup-${queue.name}".hashCode().toLong()
        }
    }

}
