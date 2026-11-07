package kolbasa.telemetry.sql

import kolbasa.Kolbasa
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import kolbasa.utils.Execution
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

internal object SqlDumpHelper {

    fun dumpQuery(
        queue: Queue<*>,
        nodeId: NodeId,
        kind: StatementKind,
        query: String,
        execution: Execution,
        affectedRows: Int
    ) {
        val config = Kolbasa.sqlDumpConfig

        if (!config.enabled) {
            return
        }

        val kinds = config.queues[queue.name] ?: return

        if (kind !in kinds) {
            return
        }

        // Sql dumps are enabled, and we need to dump these types of queries
        val text = buildString {
            appendLine("---------------------------------------------")
            append("Date: ").append(formatStartTime(execution)).appendLine()
            append("Queue: ").append(queue.name).appendLine()
            // Only a cluster has nodes to name
            if (nodeId != NodeId.EMPTY_NODE_ID) {
                append("Node: ").append(nodeId.id).appendLine()
            }
            append("Kind: ").append(kind).appendLine()
            append("Duration: ").append(formatDuration(execution)).appendLine()
            append("Rows: ").append(affectedRows).appendLine()
            appendLine(query)
        }

        config.writer.run {
            write(text)
            flush()
        }
    }

    // Milliseconds with microseconds, "0.412ms": many statements take less than a millisecond.
    // Locale.ROOT keeps the decimal point a point
    private fun formatDuration(execution: Execution): String {
        return String.format(Locale.ROOT, "%.3fms", execution.durationNanos / 1_000_000.0)
    }

    private fun formatStartTime(execution: Execution): String {
        val instant = Instant.ofEpochMilli(execution.startTimeEpochMillis)
        val zone = ZoneId.systemDefault()
        val dateTime = LocalDateTime.ofInstant(instant, zone)
        return dateTime.format(dateTimeFormatter)
    }

    private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

}
