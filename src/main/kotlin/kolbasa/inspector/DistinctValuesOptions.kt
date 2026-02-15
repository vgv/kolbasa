package kolbasa.inspector

import kolbasa.consumer.filter.Condition
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
) {

    init {
        Checks.checkSamplePercent(samplePercent)
    }

    companion object {

        /** Default options: automatic sampling, no filter. */
        val DEFAULT = DistinctValuesOptions()

        /** @see CountOptions.YOU_KNOW_BETTER */
        const val YOU_KNOW_BETTER = CountOptions.YOU_KNOW_BETTER
    }

}
