package kolbasa.cluster

import kolbasa.queue.Checks
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory

data class ClusterStateUpdateConfig(
    val interval: Duration = Duration.ofMinutes(1),
    val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DEFAULT_THREAD_FACTORY)
) {

    init {
        Checks.checkClusterStateUpdateInterval(interval)
    }

    internal companion object {

        val MIN_INTERVAL: Duration = Duration.ofSeconds(1)

        val DEFAULT_THREAD_FACTORY = ThreadFactory { runnable ->
            val thread = Thread(runnable, "kolbasa-cluster-state-updater")
            thread.isDaemon = true
            thread
        }
    }
}
