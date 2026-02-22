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
