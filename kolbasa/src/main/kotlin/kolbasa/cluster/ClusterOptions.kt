package kolbasa.cluster

import kolbasa.queue.Checks
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

/**
 * Options of a [Cluster].
 *
 * The defaults fit most applications: the state is read every minute, on one background thread shared by all
 * clusters in the JVM, and a failed read goes to the log.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = ClusterOptions(stateUpdateInterval = Duration.ofSeconds(30))
 *
 * val cluster = Cluster(listOf(nodeA, nodeB, nodeC), listOf(orders), options)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = ClusterOptions.builder()
 *     .stateUpdateInterval(Duration.ofSeconds(30))
 *     .build();
 *
 * var cluster = new Cluster(List.of(nodeA, nodeB, nodeC), List.of(orders), options);
 * ```
 */
data class ClusterOptions(
    /**
     * How often the state is read again, see [Cluster.initAndScheduleStateUpdate]. After an operator moves a shard, it
     * takes up to this long for the cluster to notice. The default is 1 minute. It must be from 1 second to 15 minutes.
     */
    val stateUpdateInterval: Duration = DEFAULT_STATE_UPDATE_INTERVAL,

    /**
     * Runs the scheduled state updates. By default, one background (daemon) thread shared by all clusters in the JVM.
     *
     * The cluster never shuts this executor down. The default one lives as long as the JVM, and your own one is yours
     * to shut down.
     *
     * If your application works with several clusters, it may make sense to give each one its own executor. On a
     * shared thread, an update that hangs, for example on a node that doesn't answer, can hold up the updates of the
     * other clusters.
     */
    val stateUpdateExecutor: ScheduledExecutorService = DEFAULT_STATE_UPDATE_EXECUTOR,

    /**
     * What to do when a scheduled state update fails. By default, [StateUpdateErrorHandler.SYSTEM_LOG].
     */
    val stateUpdateErrorHandler: StateUpdateErrorHandler = StateUpdateErrorHandler.SYSTEM_LOG
) {

    init {
        Checks.checkClusterStateUpdateInterval(stateUpdateInterval)
    }

    /**
     * Builder for flexible [ClusterOptions] creation, when only some of the properties need to be set.
     */
    class Builder {
        private var stateUpdateInterval: Duration = DEFAULT_STATE_UPDATE_INTERVAL
        private var stateUpdateExecutor: ScheduledExecutorService = DEFAULT_STATE_UPDATE_EXECUTOR
        private var stateUpdateErrorHandler: StateUpdateErrorHandler = StateUpdateErrorHandler.SYSTEM_LOG

        /**
         * Sets [ClusterOptions.stateUpdateInterval] – how often the state is read again.
         */
        fun stateUpdateInterval(stateUpdateInterval: Duration) = apply {
            this.stateUpdateInterval = stateUpdateInterval
        }

        /**
         * Sets [ClusterOptions.stateUpdateExecutor] – what runs the scheduled state updates.
         */
        fun stateUpdateExecutor(stateUpdateExecutor: ScheduledExecutorService) = apply {
            this.stateUpdateExecutor = stateUpdateExecutor
        }

        /**
         * Sets [ClusterOptions.stateUpdateErrorHandler] – what to do when a scheduled state update fails.
         */
        fun stateUpdateErrorHandler(stateUpdateErrorHandler: StateUpdateErrorHandler) = apply {
            this.stateUpdateErrorHandler = stateUpdateErrorHandler
        }

        /**
         * Creates a new [ClusterOptions] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = ClusterOptions(
            stateUpdateInterval = stateUpdateInterval,
            stateUpdateExecutor = stateUpdateExecutor,
            stateUpdateErrorHandler = stateUpdateErrorHandler
        )
    }

    companion object {

        /**
         * How often the state is read again, unless [stateUpdateInterval] says otherwise.
         */
        @JvmField
        val DEFAULT_STATE_UPDATE_INTERVAL: Duration = Duration.ofMinutes(1)

        /**
         * The shortest allowed [stateUpdateInterval].
         */
        @JvmField
        val MIN_STATE_UPDATE_INTERVAL: Duration = Duration.ofSeconds(1)

        /**
         * The longest allowed [stateUpdateInterval].
         */
        @JvmField
        val MAX_STATE_UPDATE_INTERVAL: Duration = Duration.ofMinutes(15)

        // One executor for every cluster in the JVM. Its thread starts with the first scheduled update
        private val DEFAULT_STATE_UPDATE_EXECUTOR: ScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor { runnable ->
                Thread(runnable, "kolbasa-cluster-state-updater").apply { isDaemon = true }
            }

        /**
         * Default options: the state is read every minute, on the shared thread, errors go to the log.
         */
        @JvmField
        val DEFAULT = ClusterOptions()

        @JvmStatic
        fun builder() = Builder()
    }
}
