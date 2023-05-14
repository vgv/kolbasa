package kolbasa

import kolbasa.queue.Queue
import kolbasa.stats.task.DeleteOutdatedMeasuresTask
import kolbasa.stats.task.DeleteOutdatedQueuesTask
import kolbasa.stats.task.UpdateAllStatsTask
import kolbasa.stats.task.UpdateRealtimeStatsTask
import kolbasa.task.CleanupConfig
import kolbasa.task.DefaultExecutor
import kolbasa.task.StatsConfig
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService

object Kolbasa {

    @JvmStatic
    @Volatile
    var executor: ScheduledExecutorService = DefaultExecutor

    @JvmStatic
    @Volatile
    var statsConfig: StatsConfig = StatsConfig()

    @JvmStatic
    @Volatile
    var cleanupConfig: CleanupConfig = CleanupConfig()

    private val knownQueues = ConcurrentHashMap<String, Queue<*, *>>()

    fun registerQueue(queue: Queue<*, *>) {
        // initialize Kolbasa first, if needed
        if (!KolbasaInitialization.initialized) {
            KolbasaInitialization.initialize()
        }

        // Add queue to the storage
        knownQueues[queue.name] = queue
    }
}

private object KolbasaInitialization {

    @Volatile
    var initialized: Boolean = false

    @Synchronized
    fun initialize() {
        if (initialized) {
            return
        }

        // Schedule all tasks
        UpdateRealtimeStatsTask().reschedule()
        UpdateAllStatsTask().reschedule()
        DeleteOutdatedMeasuresTask().reschedule()
        DeleteOutdatedQueuesTask().reschedule()

        initialized = true
    }

}


