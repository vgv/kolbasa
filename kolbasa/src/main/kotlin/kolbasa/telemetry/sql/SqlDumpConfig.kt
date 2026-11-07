package kolbasa.telemetry.sql

import kolbasa.queue.Queue
import java.io.Writer
import java.util.*

/**
 * Says whether kolbasa prints the SQL it runs, for which queues and statements, and where to.
 *
 * Off by default. Set it with [Kolbasa.sqlDumpConfig][kolbasa.Kolbasa.sqlDumpConfig]. It is read on every statement,
 * so a change takes effect at once.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.sqlDumpConfig = SqlDumpConfig.builder()
 *     .enable()
 *     .writer(PrintWriter(System.out))
 *     .queue(ordersQueue, StatementKind.CONSUMER_SELECT, StatementKind.CONSUMER_DELETE)
 *     .build()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setSqlDumpConfig(SqlDumpConfig.builder()
 *     .enable()
 *     .writer(new PrintWriter(System.out))
 *     .queue(ordersQueue, StatementKind.CONSUMER_SELECT, StatementKind.CONSUMER_DELETE)
 *     .build());
 * ```
 */
data class SqlDumpConfig(
    /**
     * Whether anything is printed at all. Default: false.
     */
    val enabled: Boolean = false,

    /**
     * Where the records go. The default writer drops everything.
     *
     * kolbasa calls it from many threads and doesn't synchronize the calls, so the writer must be thread-safe.
     * [java.io.PrintWriter] and the other writers from the JDK are.
     *
     * kolbasa calls `flush()` after every record. With a lot of records this can slow your application down. In that
     * case, write your own `Writer` that decides when to really flush. For example, it can keep the records in a
     * buffer and flush only on every 100th call. Remember that it must be thread-safe too.
     *
     * The writer must not throw. An exception from it fails the call that ran the statement, for example `send()`.
     */
    val writer: Writer = Writer.nullWriter(),

    /**
     * Which statements to print, by [Queue.name]. A queue that is not here prints nothing.
     */
    val queues: Map<String, EnumSet<StatementKind>> = emptyMap()
) {

    /**
     * Builder for [SqlDumpConfig].
     */
    class Builder internal constructor() {
        private var enabled: Boolean = false
        private var writer: Writer = Writer.nullWriter()
        private val queues: MutableMap<String, EnumSet<StatementKind>> = mutableMapOf()

        /**
         * Turns the dump on, see [SqlDumpConfig.enabled].
         */
        fun enable() = apply { this.enabled = true }

        /**
         * Turns the dump off, see [SqlDumpConfig.enabled].
         */
        fun disable() = apply { this.enabled = false }

        /**
         * Sets [SqlDumpConfig.writer] – where the records go.
         */
        fun writer(writer: Writer) = apply { this.writer = writer }

        /**
         * Prints the statements of [kind] for [queue]. Without any kind, every statement of the queue is printed.
         *
         * Calling it again for the same queue replaces its kinds.
         */
        fun queue(queue: Queue<*>, vararg kind: StatementKind) = apply {
            queues[queue.name] = if (kind.isEmpty()) {
                EnumSet.allOf(StatementKind::class.java)
            } else {
                EnumSet.copyOf(kind.asList())
            }
        }

        /**
         * Creates a new [SqlDumpConfig]. Later calls to this builder don't change it.
         */
        fun build() = SqlDumpConfig(enabled, writer, queues.toMap())
    }

    companion object {
        /**
         * Creates a new [Builder], with the dump off and no queues.
         */
        @JvmStatic
        fun builder(): Builder = Builder()
    }

}

/**
 * A kind of SQL statement that kolbasa runs. [SqlDumpConfig] prints only the kinds you choose for a queue.
 */
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
