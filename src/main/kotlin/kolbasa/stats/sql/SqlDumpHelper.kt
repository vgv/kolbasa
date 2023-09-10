package kolbasa.stats.sql

import kolbasa.Kolbasa
import kolbasa.queue.Queue
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

internal object SqlDumpHelper {

    fun dumpQuery(
        queue: Queue<*, *>,
        kind: StatementKind,
        query: String,
        startExecution: LocalDateTime,
        executionDuration: Duration,
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

        // Sql dumps are enabled and we need to dump these types of queries
        val text = buildString {
            appendLine("---------------------------------------------")
            append("Date: ").append(startExecution.format(dateTimeFormatter)).appendLine()
            append("Duration: ").append(executionDuration.toMillis()).append("ms").appendLine()
            append("Rows: ").append(affectedRows).appendLine()
            appendLine(query)
        }

        config.writer.run {
            write(text)
            flush()
        }
    }

    private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

}
