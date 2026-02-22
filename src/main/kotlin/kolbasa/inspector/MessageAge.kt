package kolbasa.inspector

import java.time.Duration

/**
 * Information about the age of messages currently in a queue.
 *
 * All durations represent the time elapsed since the message was created or scheduled.
 * All properties are `null` when the queue is empty or has no messages matching the criteria.
 */
data class MessageAge(
    /**
     * Age of the oldest message in the queue (the one inserted first).
     */
    val oldest: Duration?,

    /**
     * Age of the newest message in the queue (the one inserted last).
     */
    val newest: Duration?,

    /**
     * Age of the oldest message that is ready to be processed (in `READY` or `RETRY` state).
     */
    val oldestReady: Duration?
) {

    companion object {

        /**
         * Merges multiple [MessageAge] instances: picks the maximum [oldest] and [oldestReady]
         * (the longest age across all sources) and the minimum [newest] (the most recently created message).
         */
        fun Iterable<MessageAge>.merge(): MessageAge {
            var oldest: Duration? = null
            var newest: Duration? = null
            var oldestReady: Duration? = null

            for (age in this) {
                val currOldest = age.oldest
                if (currOldest != null && (oldest == null || oldest < currOldest)) {
                    oldest = currOldest
                }

                val currNewest = age.newest
                if (currNewest != null && (newest == null || newest > currNewest)) {
                    newest = currNewest
                }

                val currOldestReady = age.oldestReady
                if (currOldestReady != null && (oldestReady == null || oldestReady < currOldestReady)) {
                    oldestReady = currOldestReady
                }
            }

            return MessageAge(oldest = oldest, newest = newest, oldestReady = oldestReady)
        }
    }

}
