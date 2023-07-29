package kolbasa.task

import kolbasa.queue.Checks
import kolbasa.queue.Queue
import kolbasa.schema.Const

data class CleanupConfig(
    /**
     * Do we need to remove outdated records "in place", i.e. when consuming records?
     * It is possible to completely disable "in place" cleanup and do it at your own schedule
     */
    val enabled: Boolean = true,
    /**
     * Max rows to delete at each cleanup iteration
     */
    val cleanupLimit: Int = Const.DEFAULT_CLEANUP_ROWS,
    /**
     * Max iterations at each cleanup
     */
    val cleanupMaxIterations: Int = Const.DEFAULT_CLEANUP_ITERATIONS,
    /**
     * What is the probability that the cleanup will trigger?
     * Default is 20%, so, every fifth consuming will trigger the cleanup process
     */
    val cleanupProbabilityPercent: Int = Const.DEFAULT_CLEANUP_PROBABILITY_PERCENT,
    /**
     * Kolbasa uses PG advisory locks to prevent cleanup one queue by multiple processes at the same time.
     * By default, we use simple lock id generator based on queue name. However, if your code actively uses
     * PG advisory locks, you may need to override this generator and make it more aligned with your code.
     */
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
