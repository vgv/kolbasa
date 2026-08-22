package kolbasa.inspector

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.SortOrder
import kolbasa.queue.Checks

/**
 * Options for [Inspector.distinctValues][kolbasa.inspector.datasource.Inspector.distinctValues].
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

    class Builder internal constructor() {
        private var samplePercent: Float = YOU_KNOW_BETTER
        private var filter: Condition? = null
        private var order: SortOrder? = null

        fun samplePercent(samplePercent: Float): Builder = apply { this.samplePercent = samplePercent }
        fun filter(filter: Condition): Builder = apply { this.filter = filter }
        fun order(order: SortOrder): Builder = apply { this.order = order }

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
