package kolbasa

import kolbasa.queue.Checks
import kolbasa.queue.Queue

data class SweepConfig(
    /**
     * Do we need to remove outdated records "in place", i.e. when consuming records?
     * It is possible to completely disable "in place" sweep and do it at your own schedule
     */
    val enabled: Boolean = true,

    /**
     * Max rows to delete at each iteration
     */
    val maxRows: Int = DEFAULT_SWEEP_ROWS,

    /**
     * Max cleanup iterations at each sweep
     */
    val maxIterations: Int = DEFAULT_SWEEP_ITERATIONS,

    /**
     * How often we want to trigger a sweep?
     * Every fifth consume? Every tenth? Every hundredth?
     * Default value is 5, so, it means that every fifth consume will trigger a sweep.
     *
     * If you need to trigger a sweep at every consume, you have to use period=1 (EVERYTIME_SWEEP_PERIOD)
     */
    val period: Int = DEFAULT_SWEEP_PERIOD,

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

    class Builder internal constructor() {
        private var enabled: Boolean = true
        private var maxRows: Int = DEFAULT_SWEEP_ROWS
        private var maxIterations: Int = DEFAULT_SWEEP_ITERATIONS
        private var period: Int = DEFAULT_SWEEP_PERIOD
        private var lockIdGenerator: (queue: Queue<*, *>) -> Long = Companion::defaultLockIdGenerator

        fun enabled() = apply { this.enabled = true }
        fun disabled() = apply { this.enabled = false }
        fun maxRows(maxRows: Int) = apply { this.maxRows = maxRows }
        fun maxIterations(maxIterations: Int) = apply { this.maxIterations = maxIterations }
        fun period(period: Int) = apply { this.period = period }
        fun lockIdGenerator(lockIdGenerator: (queue: Queue<*, *>) -> Long) = apply { this.lockIdGenerator = lockIdGenerator }

        fun build() = SweepConfig(enabled, maxRows, maxIterations, period, lockIdGenerator)
    }

    companion object {

        /**
         * How many rows to delete at each iteration during sweep
         */
        const val MIN_SWEEP_ROWS = 100
        const val DEFAULT_SWEEP_ROWS = 1_000
        const val MAX_SWEEP_ROWS = 100_000

        /**
         * How many iterations at each sweep
         */
        const val MIN_SWEEP_ITERATIONS = 1
        const val DEFAULT_SWEEP_ITERATIONS = 5
        const val MAX_SWEEP_ITERATIONS = 100

        /**
         * How often we want to trigger a sweep?
         * Every fifth consume? Every tenth? Every hundredth?
         *
         * Default value is 5, so, it means that every fifth consume will trigger a sweep.
         * If you want to trigger a sweep at every consume, you have to use period = 1 (EVERYTIME_SWEEP_PERIOD)
         */
        const val EVERYTIME_SWEEP_PERIOD = 1
        const val MIN_SWEEP_PERIOD = EVERYTIME_SWEEP_PERIOD
        const val DEFAULT_SWEEP_PERIOD = 5
        const val MAX_SWEEP_PERIOD = 1_000_000_000

        @JvmStatic
        fun builder(): Builder = Builder()

        private fun defaultLockIdGenerator(queue: Queue<*, *>): Long {
            return "sweep-${queue.name}".hashCode().toLong()
        }
    }

}
