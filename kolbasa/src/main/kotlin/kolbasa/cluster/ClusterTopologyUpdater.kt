package kolbasa.cluster

import java.util.concurrent.TimeUnit

/**
 * Keeps the topology of a [Cluster] fresh: reads it now, and then again and again, on a schedule.
 *
 * The topology is which nodes the cluster has and which node owns which shard. It changes when an operator moves
 * shards between nodes, or when a node is added. [Cluster.updateTopology] reads it once. This class calls it on a
 * schedule, so [ClusterProducer], [ClusterConsumer], [ClusterMutator] and [ClusterInspector] follow the changes
 * on their own.
 *
 * Create one updater per [Cluster], at application startup, and call [start] before you use any of these four
 * classes. If you already have a scheduler, Spring's `TaskScheduler` for example, you don't need this class: call
 * [Cluster.updateTopology] from your scheduler instead.
 *
 * Call [close] when you are done with the updater: it stops the scheduled updates. This matters when the updater
 * lives shorter than the JVM, in tests for example. Spring, for example, calls it on its own when the context closes.
 * The cluster itself has nothing to close: it keeps working on the last topology read.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val cluster = Cluster(listOf(nodeA, nodeB, nodeC), listOf(orders))
 *
 * // Read the topology now, and keep it fresh in the background
 * val updater = ClusterTopologyUpdater(cluster)
 * updater.start()
 *
 * val producer: Producer = ClusterProducer(cluster)
 * producer.send(orders, "message")
 *
 * // When the application stops
 * updater.close()
 * ```
 *
 * @param cluster the cluster to keep fresh
 * @param options how often to update it, on what executor, and what to do with errors, see
 * [ClusterTopologyUpdaterOptions]
 */
class ClusterTopologyUpdater @JvmOverloads constructor(
    private val cluster: Cluster,
    private val options: ClusterTopologyUpdaterOptions = ClusterTopologyUpdaterOptions.DEFAULT
) : AutoCloseable {

    private var lifecycle = Lifecycle.NEW

    /**
     * Reads the topology now, and schedules the next reads.
     *
     * It runs [Cluster.updateTopology] immediately, so the cluster is ready to use when the method returns.
     * Then it schedules the next updates on [ClusterTopologyUpdaterOptions.executor], with
     * [ClusterTopologyUpdaterOptions.interval] between them, until [close].
     *
     * If this first update fails, the method throws and schedules nothing: without a topology the cluster can't work.
     * Then the method can be called again, for example when the database is back. A later update that fails, for
     * example because a node is down, keeps the old topology. Its error goes to
     * [ClusterTopologyUpdaterOptions.errorHandler], and the next update runs as usual.
     *
     * Once the updates are scheduled, a second call does nothing. After [close] the method throws
     * [IllegalStateException].
     */
    @Synchronized
    fun start() {
        when (lifecycle) {
            Lifecycle.NEW -> {}
            Lifecycle.SCHEDULED -> return
            Lifecycle.CLOSED -> error("Cluster topology updater is closed")
        }

        // If it throws, nothing is scheduled, and the method can be called again
        cluster.updateTopology()
        scheduleNextUpdate()

        lifecycle = Lifecycle.SCHEDULED
    }

    private fun scheduleNextUpdate() {
        options.executor.schedule(
            { updateOnSchedule() },
            options.interval.toMillis(),
            TimeUnit.MILLISECONDS
        )
    }

    // Synchronized like close(), so no update starts after close() has returned
    @Synchronized
    private fun updateOnSchedule() {
        // Closed: return before try, so finally doesn't schedule the next update, and the chain ends here
        if (lifecycle == Lifecycle.CLOSED) {
            return
        }

        try {
            cluster.updateTopology()
        } catch (e: Exception) {
            // The old topology stays until the next update
            options.errorHandler.onTopologyUpdateError(TopologyUpdateError(e))
        } finally {
            // Always, even after a failure or a handler that throws: one failure must not stop all the next updates
            scheduleNextUpdate()
        }
    }

    /**
     * Stops the scheduled updates.
     *
     * If an update is running, the method waits for it to finish, so no update runs after it returns. Usually that
     * takes a few milliseconds. On a node that doesn't answer it takes as long as the node's connection and socket
     * timeouts allow, the same as any other call to that node.
     *
     * The cluster stays usable: it keeps working on the last topology read. The data sources stay open, they are
     * yours to close. The executor isn't shut down either, see [ClusterTopologyUpdaterOptions.executor].
     *
     * A second call does nothing.
     */
    @Synchronized
    override fun close() {
        lifecycle = Lifecycle.CLOSED
    }

    // "NEW -> SCHEDULED -> CLOSED" or "NEW -> CLOSED"
    private enum class Lifecycle {
        NEW,
        SCHEDULED,
        CLOSED
    }
}
