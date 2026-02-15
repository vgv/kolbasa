package kolbasa.inspector

import kolbasa.consumer.filter.Condition
import kolbasa.queue.Checks

/**
 * Options for [Inspector.count][kolbasa.inspector.datasource.Inspector.count].
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

    companion object {

        /** Default options: automatic sampling, no filter. */
        val DEFAULT = CountOptions()

        /**
         * A magic constant meaning "I trust Kolbasa to pick the right sampling percent for me."
         *
         * When this value is used, Kolbasa will estimate a reasonable sampling level based on the table size,
         * keeping the balance between accuracy and speed. In most cases this is the best choice — you only
         * need to set an explicit percent if you have specific requirements.
         */
        const val YOU_KNOW_BETTER = Float.POSITIVE_INFINITY
    }

}
