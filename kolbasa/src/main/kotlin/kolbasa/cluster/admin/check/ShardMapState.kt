package kolbasa.cluster.admin.check

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.admin.MoveHelpers
import kolbasa.cluster.admin.ShardCopyResult
import kolbasa.cluster.admin.ShardMapResult
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Compare the copies of the shard map on the nodes, and look for the rows that can't be right.
 *
 * A node of [nodes] that is not in [copies] has no shard table.
 */
internal class ShardMapState(
    private val nodes: Set<NodeId>,
    private val mergedShards: Map<ShardId, Shard>,
    private val copies: Map<NodeId, Map<ShardId, Shard>>,
    private val invalidShards: Map<NodeId, List<Int>>
) {

    fun compute(): ShardMapResult {
        return ShardMapResult(
            copies = copies.mapValues { (_, copy) -> compareCopy(copy) },
            nodesWithoutTable = nodes - copies.keys,
            shardsWithoutRow = ShardId.SHARDS_RANGE.map { ShardId.of(it) }.filter { it !in mergedShards }.toSet(),
            invalidShards = invalidShards.filterValues { it.isNotEmpty() },
            unknownOwners = findUnknownOwners()
        )
    }

    private fun compareCopy(copy: Map<ShardId, Shard>): ShardCopyResult {
        val missing = mutableListOf<ShardId>()
        val outdated = mutableListOf<ShardId>()
        val conflicting = mutableListOf<ShardId>()

        for ((shardId, newestRow) in mergedShards.toSortedMap()) {
            val row = copy[shardId]
            when {
                row == null -> missing += shardId
                row.revision < newestRow.revision -> outdated += shardId
                row != newestRow -> conflicting += shardId
            }
        }

        return ShardCopyResult(missing, outdated, conflicting)
    }

    private fun findUnknownOwners(): Map<NodeId, List<ShardId>> {
        val unknownOwners = sortedMapOf<NodeId, MutableList<ShardId>>()

        // A stable shard has one owner. A migrating one has only the node it is going to, which is its producer too
        for ((shardId, shard) in mergedShards.toSortedMap()) {
            if (shard.producerNode !in nodes) {
                unknownOwners.computeIfAbsent(shard.producerNode) { mutableListOf() }.add(shardId)
            }
        }

        return unknownOwners
    }

    companion object {
        fun check(dataSources: List<DataSource>): ShardMapResult {
            val nodes = ClusterHelper.readNodes(dataSources)
            val shardMap = MoveHelpers.readShards(nodes)
            val invalidShards = MoveHelpers.readInvalidShards(nodes, shardMap)

            return ShardMapState(
                nodes = nodes.keys.map { it.id }.toSet(),
                mergedShards = shardMap.mergedShards,
                copies = shardMap.copies.mapKeys { (node, _) -> node.id },
                invalidShards = invalidShards.mapKeys { (node, _) -> node.id }
            ).compute()
        }
    }
}
