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
     * 2) 'flush' is called after every query dumped. If you need to dump a lot of queries it can cause performance
     * degradation. In this case, you need to implement a more sophisticated Writer with custom 'flush' behaviour (like
     * internal buffering and do real flush at every 1000th invocation or something like this)
     */
    val writer: Writer = Writer.nullWriter(),

    /**
     * Queues you want to monitor and dump related SQL queries
     */
    val queues: Map<String, EnumSet<StatementKind>> = emptyMap()
) {

    constructor(writer: Writer, queues: Map<Queue<*, *>, List<StatementKind>>) : this(
        enabled = true,
        writer = writer,
        queues = queues.map { it.key.name to EnumSet.copyOf(it.value) }.toMap()
    )

    constructor(writer: Writer, queue: Queue<*, *>, vararg kind: StatementKind = StatementKind.values()) : this(
        writer = writer,
        queues = mapOf(queue to kind.toList())
    )
}

enum class StatementKind {
    PRODUCER_INSERT,
    CONSUMER_SELECT,
    CONSUMER_DELETE,
    SWEEP
}
