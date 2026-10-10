package kolbasa.inspector

import kolbasa.consumer.filter.Condition
import kolbasa.queue.Checks

/**
 * Options for [Inspector.count][kolbasa.inspector.datasource.Inspector.count].
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = CountOptions(samplePercent = 5.0f, filter = ACCOUNT_ID eq 123)
 *
 * val messages = inspector.count(queue, options)
 * ```
 *
 * The same from Java, where the filter comes from the static factories of
 * [Filter][kolbasa.consumer.filter.Filter]:
 *
 * ```java
 * var options = CountOptions.builder()
 *     .samplePercent(5.0f)
 *     .filter(Filter.eq(ACCOUNT_ID, 123))
 *     .build();
 *
 * var messages = inspector.count(queue, options);
 * ```
 */
data class CountOptions(
    /**
     * The percentage of the table to sample, in the range `(0, 100]`.
     * Defaults to [YOU_KNOW_BETTER], which lets Kolbasa choose an appropriate value automatically.
     */
    val samplePercent: Float = YOU_KNOW_BETTER,

    /**
     * An optional condition to restrict which messages are counted.
     * When `null` (the default), all messages are counted.
     */
    val filter: Condition? = null,
) {

    init {
        Checks.checkSamplePercent(samplePercent)
    }

    /** Builder for flexible [CountOptions] creation, when only some of the properties need to be set. */
    class Builder internal constructor() {
        private var samplePercent: Float = YOU_KNOW_BETTER
        private var filter: Condition? = null

        /** Sets [CountOptions.samplePercent] – the percentage of the table to sample, `(0, 100]`. */
        fun samplePercent(samplePercent: Float): Builder = apply { this.samplePercent = samplePercent }

        /** Sets [CountOptions.filter] – the condition on meta fields that restricts which messages are counted. */
        fun filter(filter: Condition): Builder = apply { this.filter = filter }

        /** Creates a new [CountOptions] instance: a fresh one on every call, nothing is cached or reused. */
        fun build(): CountOptions = CountOptions(samplePercent, filter)
    }

    companion object {

        /** Default options: automatic sampling, no filter. */
        @JvmField
        val DEFAULT = CountOptions()

        /**
         * A magic constant meaning "I trust Kolbasa to pick the right sampling percent for me."
         *
         * When this value is used, Kolbasa will estimate a reasonable sampling level based on the table size,
         * keeping the balance between accuracy and speed. In most cases this is the best choice — you only
         * need to set an explicit percent if you have specific requirements.
         */
        const val YOU_KNOW_BETTER = Float.POSITIVE_INFINITY

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
