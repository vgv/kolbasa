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
     *
     * Default value is `0.0001 (1 / 10_000)`, so, it means that every ten thousandth consume will trigger a sweep.
     * If you want to trigger a sweep at every consume, you have to use `probability = 1.0f`, to disable automatic sweep
     * completely and manage it manually use `probability = 0.0f`
     */
    val probability: Double = DEFAULT_SWEEP_PROBABILITY,
) {

    init {
        Checks.checkSweepMaxMessages(maxMessages)
        Checks.checkSweepProbability(probability)
    }

    class Builder internal constructor() {
        private var enabled: Boolean = true
        private var maxMessages: Int = DEFAULT_SWEEP_MESSAGES
        private var probability: Double = DEFAULT_SWEEP_PROBABILITY

        fun enabled() = apply { this.enabled = true }
        fun disabled() = apply { this.enabled = false }
        fun maxMessages(maxMessages: Int) = apply { this.maxMessages = maxMessages }
        fun probability(probability: Double) = apply { this.probability = probability }

        fun build() = SweepConfig(enabled, maxMessages, probability)
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
         * Default value is `0.0001 (1 / 10_000)`, so, it means that every ten thousandth consume will trigger a sweep.
         * If you want to trigger a sweep at every consume, you have to use `probability = 1.0`, to disable automatic sweep
         * completely and manage it manually use `probability = 0.0`
         */
        const val MIN_SWEEP_PROBABILITY = 0.0
        const val DEFAULT_SWEEP_PROBABILITY = 1.0 / 10_000
        const val MAX_SWEEP_PROBABILITY = 1.0

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
