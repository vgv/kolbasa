package kolbasa.consumer.sweep

import kolbasa.queue.Checks

data class SweepConfig(
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
        private var maxMessages: Int = DEFAULT_SWEEP_MESSAGES
        private var probability: Double = DEFAULT_SWEEP_PROBABILITY

        fun disable() = apply { this.probability = SWEEP_IS_DISABLED }
        fun maxMessages(maxMessages: Int) = apply { this.maxMessages = maxMessages }
        fun probability(probability: Double) = apply { this.probability = probability }

        fun build() = SweepConfig(maxMessages, probability)
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
         * If you want to trigger a sweep at every consume, you have to use `probability = 1.0` (SWEEP_IS_ALWAYS_ON constant),
         * to disable automatic sweep completely and manage it manually use `probability = 0.0` (SWEEP_IS_DISABLED constant)
         */
        const val MIN_SWEEP_PROBABILITY = 0.0
        const val DEFAULT_SWEEP_PROBABILITY = 1.0 / 10_000
        const val MAX_SWEEP_PROBABILITY = 1.0

        // Nice mnemonic constants
        const val SWEEP_IS_DISABLED = MIN_SWEEP_PROBABILITY
        const val SWEEP_IS_ALWAYS_ON = MAX_SWEEP_PROBABILITY

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
