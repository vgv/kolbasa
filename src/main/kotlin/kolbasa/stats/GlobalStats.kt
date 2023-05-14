package kolbasa.stats

import kolbasa.queue.Queue
import java.util.concurrent.ConcurrentHashMap

/**
 * One global statistic storage, for all queues and measures
 */
internal object GlobalStats {

    // [QueueName => QueueStats]
    private val queueStats = ConcurrentHashMap<String, QueueStats>()

    fun getStatsForQueue(queue: Queue<*, *>): QueueStats {
        val queueName = queue.name

        return queueStats.computeIfAbsent(queueName) { _ ->
            QueueStats(queue)
        }
    }

    fun dumpAndReset(onlyRealtimeDumps: Boolean): List<QueueDump> {
        return queueStats.values.map { queueStats ->
            queueStats.dumpAndReset(onlyRealtimeDumps)
        }
    }
}
