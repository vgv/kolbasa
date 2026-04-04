package kolbasa.stats.sql

import kolbasa.queue.Queue
import java.io.Writer
import java.util.*

data class SqlDumpConfig(
    /**
     * Do we want to dump generated SQL queries?
     */
    val enabled: Boolean = false,

    /**
     * Writer where to dump sql queries
     *
     * Two things to remember:
     * 1) This library doesn't do any synchronization around this writer. All threads just use this 'writer' instance
     * without synchronization. If your 'writer' implementation isn't thread-safe – please, take care of that yourself.
     * 2) 'flush' is called after EVERY query dumped. If you need to dump a lot of queries it can cause performance
     * degradation. In this case, you need to implement a more sophisticated Writer with custom 'flush' behaviour (like
     * internal buffering and do real flush at every 100th invocation or something like this)
     */
    val writer: Writer = Writer.nullWriter(),

    /**
     * Queues you want to monitor and dump related SQL queries
     */
    val queues: Map<String, EnumSet<StatementKind>> = emptyMap()
) {

    class Builder internal constructor() {
        private var enabled: Boolean = false
        private var writer: Writer = Writer.nullWriter()
        private var queues: MutableMap<String, EnumSet<StatementKind>> = mutableMapOf()

        fun enabled() = apply { this.enabled = true }
        fun disabled() = apply { this.enabled = false }
        fun writer(writer: Writer) = apply { this.writer = writer }
        fun queue(queue: Queue<*>, vararg kind: StatementKind) = apply {
            queues[queue.name] = EnumSet.copyOf(kind.toList())
        }

        fun build() = SqlDumpConfig(enabled, writer, queues)
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = Builder()
    }

}

enum class StatementKind {
    /**
     * Batch INSERT of messages into a queue table
     */
    PRODUCER_INSERT,

    /**
     * SELECT to receive messages
     */
    CONSUMER_SELECT,

    /**
     * DELETE of successfully processed messages
     */
    CONSUMER_DELETE,

    /**
     * Atomic DELETE (main queue) + INSERT (archive queue) that moves acknowledged messages from a main queue to its archive
     */
    CONSUMER_DELETE_TO_ARCHIVE,

    /**
     * DELETE of dead messages (remaining_attempts <= 0) during probabilistic sweep
     */
    SWEEP,

    /**
     * Atomic DELETE (main queue) + INSERT (dlq) that moves dead messages from a main queue to its DLQ during sweep
     */
    SWEEP_TO_DLQ,

    /**
     * DELETE of expired messages from a DLQ or Archive queue based on retention duration or max message count
     */
    RETENTION_CLEANUP,

    /**
     * UPDATE of message fields by specific message ids
     */
    MUTATE_BY_ID,

    /**
     * UPDATE of message fields by filter condition
     */
    MUTATE_BY_FILTER
}
