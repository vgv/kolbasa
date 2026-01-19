package kolbasa.consumer.sweep

import kolbasa.queue.Checks

data class SweepConfig(
    /**
     * Do we need to remove outdated records "in place", i.e. when consuming records?
     * It is possible to completely disable "in place" sweep and do it at your own schedule
     */
    val enabled: Boolean = true,

    /**
     * Max messages to delete during sweep
     */
    val maxMessages: Int = DEFAULT_SWEEP_MESSAGES,

    /**
     * How often we want to trigger a sweep?
     * Every fifth consume? Every tenth? Every hundredth?
     * Default value is 10_000, so, it means that every ten thousandth consume will trigger a sweep.
     *
     * If you need to trigger a sweep at every consume, you have to use period=1 (EVERYTIME_SWEEP_PERIOD)
     */
    val period: Int = DEFAULT_SWEEP_PERIOD,
) {

    init {
        Checks.checkSweepMaxMessages(maxMessages)
        Checks.checkSweepPeriod(period)
    }

    class Builder internal constructor() {
        private var enabled: Boolean = true
        private var maxMessages: Int = DEFAULT_SWEEP_MESSAGES
        private var period: Int = DEFAULT_SWEEP_PERIOD

        fun enabled() = apply { this.enabled = true }
        fun disabled() = apply { this.enabled = false }
        fun maxMessages(maxMessages: Int) = apply { this.maxMessages = maxMessages }
        fun period(period: Int) = apply { this.period = period }

        fun build() = SweepConfig(enabled, maxMessages, period)
    }

    companion object {

        /**
         * How many messages to delete during sweep
         */
        const val MIN_SWEEP_MESSAGES = 100
        const val DEFAULT_SWEEP_MESSAGES = 10_000
        const val MAX_SWEEP_MESSAGES = 100_000

        /**
         * How often we want to trigger a sweep?
         * Every fifth consume? Every tenth? Every hundredth?
         *
         * Default value is 10_000, so, it means that every ten thousandth consume will trigger a sweep.
         * If you want to trigger a sweep at every consume, you have to use period = 1 (EVERYTIME_SWEEP_PERIOD)
         */
        const val EVERYTIME_SWEEP_PERIOD = 1
        const val MIN_SWEEP_PERIOD = EVERYTIME_SWEEP_PERIOD
        const val DEFAULT_SWEEP_PERIOD = 10_000
        const val MAX_SWEEP_PERIOD = 1_000_000_000

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
