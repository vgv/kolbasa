package kolbasa.cluster

import kolbasa.queue.Checks
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

/**
 * Options of a [ClusterTopologyUpdater].
 *
 * The defaults fit most applications: the topology is read every minute, on one background thread shared by all
 * updaters in the JVM, and a failed read goes to the log.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = ClusterTopologyUpdaterOptions(interval = Duration.ofSeconds(30))
 *
 * val updater = ClusterTopologyUpdater(cluster, options)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = ClusterTopologyUpdaterOptions.builder()
 *     .interval(Duration.ofSeconds(30))
 *     .build();
 *
 * var updater = new ClusterTopologyUpdater(cluster, options);
 * ```
 */
data class ClusterTopologyUpdaterOptions(
    /**
     * How often the topology is read again. After an operator moves a shard, it takes up to this long for the cluster
     * to notice. The default is [DEFAULT_INTERVAL]. It must be from [MIN_INTERVAL] to [MAX_INTERVAL].
     */
    val interval: Duration = DEFAULT_INTERVAL,

    /**
     * Runs the scheduled updates. By default, one background (daemon) thread shared by all updaters in the JVM.
     *
     * The updater never shuts this executor down. The default one lives as long as the JVM, and your own one is yours
     * to shut down.
     *
     * If your application works with several clusters, it may make sense to give each updater its own executor. On a
     * shared thread, an update that hangs, for example on a node that doesn't answer, can hold up the updates of the
     * other clusters.
     */
    val executor: ScheduledExecutorService = DEFAULT_EXECUTOR,

    /**
     * What to do when a scheduled update fails. By default, [TopologyUpdateErrorHandler.SYSTEM_LOG].
     */
    val errorHandler: TopologyUpdateErrorHandler = TopologyUpdateErrorHandler.SYSTEM_LOG
) {

    init {
        Checks.checkClusterTopologyUpdateInterval(interval)
    }

    /**
     * Builder for flexible [ClusterTopologyUpdaterOptions] creation, when only some of the properties need to be set.
     */
    class Builder {
        private var interval: Duration = DEFAULT_INTERVAL
        private var executor: ScheduledExecutorService = DEFAULT_EXECUTOR
        private var errorHandler: TopologyUpdateErrorHandler = TopologyUpdateErrorHandler.SYSTEM_LOG

        /**
         * Sets [ClusterTopologyUpdaterOptions.interval] – how often the topology is read again.
         */
        fun interval(interval: Duration) = apply {
            this.interval = interval
        }

        /**
         * Sets [ClusterTopologyUpdaterOptions.executor] – what runs the scheduled updates.
         */
        fun executor(executor: ScheduledExecutorService) = apply {
            this.executor = executor
        }

        /**
         * Sets [ClusterTopologyUpdaterOptions.errorHandler] – what to do when a scheduled update fails.
         */
        fun errorHandler(errorHandler: TopologyUpdateErrorHandler) = apply {
            this.errorHandler = errorHandler
        }

        /**
         * Creates a new [ClusterTopologyUpdaterOptions] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = ClusterTopologyUpdaterOptions(
            interval = interval,
            executor = executor,
            errorHandler = errorHandler
        )
    }

    companion object {

        /**
         * How often the topology is read again, unless [interval] says otherwise.
         */
        @JvmField
        val DEFAULT_INTERVAL: Duration = Duration.ofMinutes(1)

        /**
         * The shortest allowed [interval].
         */
        @JvmField
        val MIN_INTERVAL: Duration = Duration.ofSeconds(1)

        /**
         * The longest allowed [interval].
         */
        @JvmField
        val MAX_INTERVAL: Duration = Duration.ofMinutes(15)

        // One executor for every updater in the JVM. Its thread starts with the first scheduled update
        private val DEFAULT_EXECUTOR: ScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor { runnable ->
                Thread(runnable, "kolbasa-cluster-topology-updater").apply { isDaemon = true }
            }

        /**
         * Default options: the topology is read every minute, on the shared thread, errors go to the log.
         */
        @JvmField
        val DEFAULT = ClusterTopologyUpdaterOptions()

        @JvmStatic
        fun builder() = Builder()
    }
}
