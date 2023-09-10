package kolbasa

import kolbasa.queue.Queue
import kolbasa.utils.TimeHelper
import java.time.format.DateTimeFormatter

internal object SqlDumpHelper {

    fun dumpQuery(
        queue: Queue<*, *>,
        kind: StatementKind,
        query: String,
        execution: TimeHelper.Execution
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
            append("Date: ").append(execution.startTime.format(dateTimeFormatter)).appendLine()
            append("Duration: ").append(execution.duration.toMillis()).append("ms").appendLine()
            append("Rows: ").append(execution.affectedRows).appendLine()
            appendLine(query)
        }

        config.writer.run {
            write(text)
            flush()
        }
    }

    private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

}
