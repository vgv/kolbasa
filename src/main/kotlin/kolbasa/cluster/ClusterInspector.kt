package kolbasa.cluster

import kolbasa.consumer.order.SortOrder
import kolbasa.inspector.CountOptions
import kolbasa.inspector.DistinctValuesOptions
import kolbasa.inspector.MessageAge
import kolbasa.inspector.MessageAge.Companion.merge
import kolbasa.inspector.Messages
import kolbasa.inspector.Messages.Companion.merge
import kolbasa.inspector.connection.ConnectionAwareDatabaseInspector
import kolbasa.inspector.datasource.DatabaseInspector
import kolbasa.inspector.datasource.Inspector
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField

/**
 * An [Inspector] that reports numbers for a whole cluster.
 *
 * Every method asks all nodes and merges their answers, so you get one number for the cluster instead of one per
 * node. How they are merged depends on the question:
 * - [count] and [size] are added up over the nodes.
 * - [isEmpty] and [isDeadOrEmpty] are true only when they are true on every node.
 * - [messageAge] keeps the oldest message of the cluster and the newest message of the cluster.
 * - [distinctValues] asks every node for at most `limit` values, adds up the counts of equal values, then sorts
 *   the merged map and applies `limit` again. The limit therefore holds for the cluster, but the answer is not the
 *   exact cluster-wide top: with `limit = 10` on three nodes, a value that is 11th on each of them is cut by each
 *   node's own limit and never reaches the merge – even though its three counts added together could make it the
 *   largest of all. And when [DistinctValuesOptions.order] is not set, the nodes are asked without any `order by`,
 *   so each of them returns an arbitrary group of values and the merged result is arbitrary as well.
 *
 * The result is as approximate as the underlying calls are: [count] samples the table instead of reading all of it,
 * and the nodes are asked one after another, not at one instant.
 *
 * This class is a thin wrapper around a [Cluster]: it holds no connections of its own and creates a plain
 * [DatabaseInspector][kolbasa.inspector.datasource.DatabaseInspector] per node behind the scenes. Creating one is
 * cheap. The [Cluster] must be initialized (see [Cluster.initAndScheduleStateUpdate]) before the first call.
 *
 * This class implements the [Inspector] interface, so it's easy to use instead of [DatabaseInspector] if you need to
 * migrate from a single-node setup to a Kolbasa cluster.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val inspector: Inspector = ClusterInspector(cluster)
 *
 * val messages = inspector.count(orders)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Inspector inspector = new ClusterInspector(cluster);
 *
 * var messages = inspector.count(orders);
 * ```
 *
 * @see Cluster
 * @see kolbasa.inspector.datasource.DatabaseInspector
 */
class ClusterInspector(private val cluster: Cluster) : Inspector {

    override fun count(queue: Queue<*>, options: CountOptions): Messages {
        // The total count of messages in the queue is the sum of counts reported by all nodes.
        return getAllInspectors().map { it.count(queue, options) }.merge()
    }

    override fun <V> distinctValues(
        queue: Queue<*>,
        metaField: MetaField<V>,
        limit: Int,
        options: DistinctValuesOptions
    ): Map<V?, Long> {
        val allInspectors = getAllInspectors()

        // Merge maps by summing counts for same keys
        val merged = mutableMapOf<V?, Long>()
        for (inspector in allInspectors) {
            val nodeValues = inspector.distinctValues(queue, metaField, limit, options)
            for ((key, count) in nodeValues) {
                merged[key] = (merged[key] ?: 0L) + count
            }
        }

        // Re-sort if order is specified and re-apply limit
        val sorted = when (options.order) {
            SortOrder.ASC, SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_LAST ->
                merged.entries.sortedBy { it.value }

            SortOrder.DESC, SortOrder.DESC_NULLS_FIRST, SortOrder.DESC_NULLS_LAST ->
                merged.entries.sortedByDescending { it.value }

            null -> merged.entries.toList()
        }

        val result = LinkedHashMap<V?, Long>(minOf(limit, sorted.size))
        for (entry in sorted.take(limit)) {
            result[entry.key] = entry.value
        }
        return result
    }

    override fun size(queue: Queue<*>): Long {
        // The total size of the queue is the sum of sizes reported by all nodes
        return getAllInspectors().sumOf { it.size(queue) }
    }

    override fun isEmpty(queue: Queue<*>): Boolean {
        // A queue is considered empty if all nodes report it as empty
        return getAllInspectors().all { it.isEmpty(queue) }
    }

    override fun isDeadOrEmpty(queue: Queue<*>): Boolean {
        // A queue is considered dead or empty if all nodes report it as dead or empty
        return getAllInspectors().all { it.isDeadOrEmpty(queue) }
    }

    override fun messageAge(queue: Queue<*>): MessageAge {
        // The age of messages in the queue is determined by merging the ages reported by all nodes:
        // - The oldest message is the maximum of all oldest messages (the longest age)
        // - The newest message is the minimum of all newest messages (the most recent age)
        // - The oldest ready message is the maximum of all oldest ready messages (the longest age among ready messages)
        return getAllInspectors().map { it.messageAge(queue) }.merge()
    }

    private fun getAllInspectors(): List<Inspector> {
        val latestState = cluster.getState()
        return latestState.getInspectors(this) { _, dataSource ->
            val peer = ConnectionAwareDatabaseInspector()
            DatabaseInspector(dataSource, peer)
        }
    }
}
