package kolbasa.consumer.sweep

import kolbasa.queue.Checks

/**
 * Configuration of the sweep – the cleanup that runs from time to time inside a `receive()` call.
 *
 * One sweep pass, on the connection of the `receive()` that triggered it:
 * - takes the messages of the queue that have run out of attempts and either deletes them or, if the queue has a
 *   [DLQ][kolbasa.queue.Queue.deadLetterQueue], moves them there
 * - applies the retention of that [DLQ][kolbasa.queue.Queue.deadLetterQueue], if there is one
 *   (see [DlqOptions][kolbasa.queue.DlqOptions])
 * - applies the retention of the [archive queue][kolbasa.queue.Queue.archiveQueue], if there is one
 *   (see [ArchiveQueueOptions][kolbasa.queue.ArchiveQueueOptions])
 *
 * Because the pass runs on the caller's connection and costs that caller some latency, it is not run on every
 * receive: [probability] decides how often it happens, and [maxMessages] caps how much one pass deletes.
 *
 * ## Usage Example
 *
 * ```kotlin
 * // sweep on every hundredth receive(), up to 50_000 messages per pass
 * Kolbasa.sweepConfig = SweepConfig.builder()
 *     .probability(0.01)
 *     .maxMessages(50_000)
 *     .build()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setSweepConfig(SweepConfig.builder()
 *     .probability(0.01)
 *     .maxMessages(50_000)
 *     .build());
 * ```
 */
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
     * If you want to trigger a sweep at every consume, you have to use `probability = 1.0` ([SWEEP_IS_ALWAYS_ON] constant),
     * to disable automatic sweep completely and manage it manually use `probability = 0.0` ([SWEEP_IS_DISABLED] constant)
     */
    val probability: Double = DEFAULT_SWEEP_PROBABILITY,
) {

    init {
        Checks.checkSweepMaxMessages(maxMessages)
        Checks.checkSweepProbability(probability)
    }

    /** Builder for flexible [SweepConfig] creation, when only some of the properties need to be set. */
    class Builder internal constructor() {
        private var maxMessages: Int = DEFAULT_SWEEP_MESSAGES
        private var probability: Double = DEFAULT_SWEEP_PROBABILITY

        /**
         * Turns the automatic sweep off, which is the same as `probability(SWEEP_IS_DISABLED)`.
         *
         * Nothing is cleaned up on its own after this: expired messages, the DLQ and the archive are only swept
         * when you call [SweepHelper.sweep] yourself.
         */
        fun disable() = apply { this.probability = SWEEP_IS_DISABLED }

        /** Sets [SweepConfig.maxMessages] – the cap on how many messages one sweep pass deletes. */
        fun maxMessages(maxMessages: Int) = apply { this.maxMessages = maxMessages }

        /** Sets [SweepConfig.probability] – how often a `receive()` call triggers a sweep. */
        fun probability(probability: Double) = apply { this.probability = probability }

        /** Creates a new [SweepConfig] instance: a fresh one on every call, nothing is cached or reused. */
        fun build() = SweepConfig(maxMessages, probability)
    }

    companion object {

        /** The smallest [SweepConfig.maxMessages] Kolbasa accepts – 100 messages per pass. */
        const val MIN_SWEEP_MESSAGES = 100

        /** The [SweepConfig.maxMessages] used when none is set – 10 000 messages per pass. */
        const val DEFAULT_SWEEP_MESSAGES = 10_000

        /** The largest [SweepConfig.maxMessages] Kolbasa accepts – 100 000 messages per pass. */
        const val MAX_SWEEP_MESSAGES = 100_000

        /** The smallest [SweepConfig.probability] Kolbasa accepts – `0.0`, the same as [SWEEP_IS_DISABLED]. */
        const val MIN_SWEEP_PROBABILITY = 0.0

        /** The [SweepConfig.probability] used when none is set – `0.0001`, one receive() in ten thousand. */
        const val DEFAULT_SWEEP_PROBABILITY = 1.0 / 10_000

        /** The largest [SweepConfig.probability] Kolbasa accepts – `1.0`, the same as [SWEEP_IS_ALWAYS_ON]. */
        const val MAX_SWEEP_PROBABILITY = 1.0

        // Nice mnemonic constants

        /** [SweepConfig.probability] value that switches the automatic sweep off. */
        const val SWEEP_IS_DISABLED = MIN_SWEEP_PROBABILITY

        /** [SweepConfig.probability] value that sweeps on every single receive(). */
        const val SWEEP_IS_ALWAYS_ON = MAX_SWEEP_PROBABILITY

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
