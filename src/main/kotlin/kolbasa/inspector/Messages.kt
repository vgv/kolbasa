package kolbasa.inspector

/**
 * Message counts grouped by state, as returned by [Inspector.count][kolbasa.inspector.datasource.Inspector.count].
 *
 * Since counting is based on sampling, the values are approximate.
 *
 * @see kolbasa.queue.meta.FieldOption for a detailed description of message states and transitions
 * @see <a href="https://github.com/vgv/kolbasa/blob/main/docs/Message state transitions.md">Message state transitions</a>
 */
data class Messages(
    /**
     * Number of messages that are scheduled for future delivery and not yet eligible for processing.
     */
    val scheduled: Long,

    /**
     * Number of messages that are ready to be consumed.
     */
    val ready: Long,

    /**
     * Number of messages currently being processed by a consumer.
     */
    val inFlight: Long,

    /**
     * Number of messages that failed processing and are waiting for a retry attempt.
     */
    val retry: Long,

    /**
     * Number of messages that exhausted all retry attempts and will not be processed again.
     */
    val dead: Long
) {

    /**
     * Returns the total number of messages across all states.
     */
    fun total(): Long = scheduled + ready + inFlight + retry + dead

    companion object {

        val DEFAULT = Messages(0, 0, 0, 0, 0)

        /**
         * Merges multiple [Messages] instances by summing up counts per state.
         */
        fun Iterable<Messages>.merge(): Messages {
            var scheduled = 0L
            var ready = 0L
            var inFlight = 0L
            var retry = 0L
            var dead = 0L

            for (m in this) {
                scheduled += m.scheduled
                ready += m.ready
                inFlight += m.inFlight
                retry += m.retry
                dead += m.dead
            }

            return Messages(scheduled, ready, inFlight, retry, dead)
        }
    }

}
