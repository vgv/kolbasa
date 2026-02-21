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
)
