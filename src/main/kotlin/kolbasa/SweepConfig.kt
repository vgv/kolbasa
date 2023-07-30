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
     * How often we want to trigger a sweep?
     * Every fifth consume? Every tenth? Every hundredth?
     * Default value is 5, so, it means that every fifth consume will trigger a sweep.
     *
     * If you need to trigger a sweep at every consume, you have to use period=1 (EVERYTIME_SWEEP_PERIOD)
     */
    val period: Int = Const.DEFAULT_SWEEP_PERIOD,

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
        Checks.checkSweepPeriod(period)
    }

    private companion object {
        fun defaultLockIdGenerator(queue: Queue<*, *>): Long {
            return "sweep-${queue.name}".hashCode().toLong()
        }
    }

}
