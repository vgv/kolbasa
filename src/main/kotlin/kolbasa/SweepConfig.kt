package kolbasa

import kolbasa.queue.Checks
import kolbasa.queue.Queue
import kolbasa.schema.Const

data class SweepConfig(
    /**
     * Do we need to remove outdated records "in place", i.e. when consuming records?
     * It is possible to completely disable "in place" sweep and do it at your own schedule
     */
    val enabled: Boolean = true,
    /**
     * Max rows to delete at each iteration
     */
    val maxRows: Int = Const.DEFAULT_SWEEP_ROWS,
    /**
     * Max cleanup iterations at each sweep
     */
    val maxIterations: Int = Const.DEFAULT_SWEEP_ITERATIONS,
    /**
     * What is the probability that the sweep will trigger?
     * Default is 20%, so, every fifth consuming will trigger the sweep process
     */
    val cleanupProbabilityPercent: Int = Const.DEFAULT_SWEEP_PROBABILITY_PERCENT,
    /**
     * Kolbasa uses PG advisory locks to prevent sweeping one queue by multiple processes at the same time.
     * By default, we use simple lock id generator based on queue name. However, if your code actively uses
     * PG advisory locks, you may need to override this generator and make it more aligned with your code.
     */
    val lockIdGenerator: (queue: Queue<*, *>) -> Long = Companion::defaultLockIdGenerator
) {

    init {
        Checks.checkSweepMaxRows(maxRows)
        Checks.checkSweepMaxIterations(maxIterations)
        Checks.checkCleanupProbabilityPercent(cleanupProbabilityPercent)
    }

    private companion object {
        fun defaultLockIdGenerator(queue: Queue<*, *>): Long {
            return "sweep-${queue.name}".hashCode().toLong()
        }
    }

}
