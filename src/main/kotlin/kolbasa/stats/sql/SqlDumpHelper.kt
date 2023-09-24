package kolbasa.stats.sql

import kolbasa.Kolbasa
import kolbasa.queue.Queue
import kolbasa.utils.Execution
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

internal object SqlDumpHelper {

    fun dumpQuery(
        queue: Queue<*, *>,
        kind: StatementKind,
        query: String,
        execution: Execution
    ) {
        val config = Kolbasa.sqlDumpConfig

        if (!config.enabled) {
            return
        }

        val kinds = config.queues[queue.name] ?: return

        if (kind !in kinds) {
            return
        }

        // Sql dumps are enabled and we need to dump these types of queries
        val text = buildString {
            appendLine("---------------------------------------------")
            append("Date: ").append(formatStartTime(execution)).appendLine()
            append("Duration: ").append(durationMillis(execution)).append("ms").appendLine()
            append("Rows: ").append(execution.affectedRows).appendLine()
            appendLine(query)
        }

        config.writer.run {
            write(text)
            flush()
        }
    }

    private fun durationMillis(execution: Execution): Long = TimeUnit.NANOSECONDS.toMillis(execution.durationNanos)

    private fun formatStartTime(execution: Execution): String {
        val instant = Instant.ofEpochMilli(execution.startTimeEpochMillis)
        val zone = ZoneId.systemDefault()
        val dateTime = LocalDateTime.ofInstant(instant, zone)
        return dateTime.format(dateTimeFormatter)
    }

    private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

}
