package kolbasa.inspector

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.SortOrder
import kolbasa.queue.Checks

/**
 * Options for [Inspector.distinctValues][kolbasa.inspector.datasource.Inspector.distinctValues].
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = DistinctValuesOptions(samplePercent = 5.0f, order = SortOrder.DESC)
 *
 * val values = inspector.distinctValues(queue, ACCOUNT_ID, 100, options)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = DistinctValuesOptions.builder()
 *     .samplePercent(5.0f)
 *     .order(SortOrder.DESC)
 *     .build();
 *
 * var values = inspector.distinctValues(queue, ACCOUNT_ID, 100, options);
 * ```
 */
data class DistinctValuesOptions(
    /**
     * The percentage of the table to sample, in the range `(0, 100]`.
     * Defaults to [YOU_KNOW_BETTER], which lets Kolbasa choose an appropriate value automatically.
     */
    val samplePercent: Float = YOU_KNOW_BETTER,

    /**
     * An optional condition to restrict which messages are considered.
     * When `null` (the default), all messages are considered.
     */
    val filter: Condition? = null,

    /**
     * An optional sort order for the results, sorted by count.
     * When `null` (the default), no ordering is applied.
     */
    val order: SortOrder? = null,
) {

    init {
        Checks.checkSamplePercent(samplePercent)
    }

    /** Builder for flexible [DistinctValuesOptions] creation, when only some of the properties need to be set. */
    class Builder internal constructor() {
        private var samplePercent: Float = YOU_KNOW_BETTER
        private var filter: Condition? = null
        private var order: SortOrder? = null

        /** Sets [DistinctValuesOptions.samplePercent] – the percentage of the table to sample, `(0, 100]`. */
        fun samplePercent(samplePercent: Float): Builder = apply { this.samplePercent = samplePercent }

        /** Sets [DistinctValuesOptions.filter] – the condition on meta fields that restricts which messages are considered. */
        fun filter(filter: Condition): Builder = apply { this.filter = filter }

        /** Sets [DistinctValuesOptions.order] – how the results are sorted by their count. */
        fun order(order: SortOrder): Builder = apply { this.order = order }

        /** Creates a new [DistinctValuesOptions] instance: a fresh one on every call, nothing is cached or reused. */
        fun build(): DistinctValuesOptions = DistinctValuesOptions(samplePercent, filter, order)
    }

    companion object {

        /** Default options: automatic sampling, no filter. */
        @JvmField
        val DEFAULT = DistinctValuesOptions()

        /** @see CountOptions.YOU_KNOW_BETTER */
        const val YOU_KNOW_BETTER = CountOptions.YOU_KNOW_BETTER

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
