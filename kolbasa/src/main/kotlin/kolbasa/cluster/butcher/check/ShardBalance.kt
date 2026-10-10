package kolbasa.cluster.butcher.check

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.butcher.MoveHelpers
import kolbasa.cluster.butcher.config.Command
import kolbasa.schema.NodeId

/**
 * Propose a minimum-movement rebalance of [shards] across [knownNodes].
 *
 * A shard is counted by `producerNode`. In migration state the invariant
 * `producerNode == nextConsumerNode` holds (see [Shard]), so in-flight shards
 * are counted toward the node they are moving to. Shards whose producer is not
 * in [knownNodes] are silently skipped (orphan detection is a separate check).
 *
 * Target per-node count is `floor(N/K)` or `ceil(N/K)` where N is the number of
 * balanced shards and K is the number of known nodes. The number of ceil slots
 * equals `N mod K`; they are awarded to nodes with the largest current count
 * so the total shards that have to move is minimized.
 */
internal class ShardBalance(
    private val shards: Collection<Shard>,
    private val knownNodes: Set<NodeId>
) {

    /** Known node -> shards it currently owns (producerNode). Orphan shards are excluded. */
    private val currentDistribution: Map<NodeId, List<Shard>> = groupShardsByNode()

    /** Known node -> how many shards it should own after rebalance (floor or ceil of N/K). */
    private val targetShardsPerNode: Map<NodeId, Int> = computeTargetPerNode()

    fun compute(): ShardBalanceResult {
        if (knownNodes.isEmpty()) {
            return ShardBalanceResult(currentDistribution, emptyMap())
        }

        val surplusShards = collectSurplusShards()
        val deficitByNode = collectDeficitByNode()
        val proposedMoves = assignSurplusToDeficit(surplusShards, deficitByNode)

        return ShardBalanceResult(currentDistribution, proposedMoves)
    }

    private fun groupShardsByNode(): Map<NodeId, List<Shard>> {
        val buckets: Map<NodeId, MutableList<Shard>> = knownNodes.associateWith { mutableListOf() }
        shards.forEach { shard ->
            buckets[shard.producerNode]?.add(shard)
        }
        return buckets
    }

    /**
     * Give the `ceil` slots (there are `N mod K` of them) to the nodes with the largest
     * current count; everyone else gets `floor`. This minimizes the number of shards that
     * must leave their current node. Ties on count are broken by NodeId for determinism.
     */
    private fun computeTargetPerNode(): Map<NodeId, Int> {
        if (knownNodes.isEmpty()) return emptyMap()

        // Example for 1024 shards / 3 nodes: floor=341, ceil=342, ceilSlots=1.
        // One node should end up with 342 shards, two with 341.
        val totalBalancedShards = currentDistribution.values.sumOf { it.size }
        val floor = totalBalancedShards / knownNodes.size
        val ceil = floor + 1
        val ceilSlots = totalBalancedShards % knownNodes.size

        // Give the ceil target to the heaviest nodes. Keeping the shards that are already
        // on heavy nodes there minimizes how many shards have to move.
        // Sort heaviest-first; tie-break by NodeId for determinism.
        val rankedNodes = currentDistribution
            .entries
            .sortedWith(compareByDescending<Map.Entry<NodeId, List<Shard>>> { it.value.size }.thenBy { it.key })
            .map { it.key }

        val result = mutableMapOf<NodeId, Int>()
        rankedNodes.forEachIndexed { rank, node ->
            result[node] = if (rank < ceilSlots) ceil else floor
        }
        return result
    }

    /** Shards that must leave their current (overloaded) node, ordered by shard number. */
    private fun collectSurplusShards(): List<Shard> {
        val surplus = mutableListOf<Shard>()
        targetShardsPerNode.forEach { (node, wanted) ->
            val current = currentDistribution.getValue(node)
            if (current.size > wanted) {
                surplus += current.sortedBy { it.shard }.take(current.size - wanted)
            }
        }
        return surplus.sortedBy { it.shard }
    }

    /** Node -> number of shards it still needs to receive to reach its target. */
    private fun collectDeficitByNode(): Map<NodeId, Int> {
        val deficit = linkedMapOf<NodeId, Int>()
        targetShardsPerNode.forEach { (node, wanted) ->
            val current = currentDistribution.getValue(node).size
            if (current < wanted) {
                deficit[node] = wanted - current
            }
        }
        return deficit
    }

    private fun assignSurplusToDeficit(
        surplus: List<Shard>,
        deficit: Map<NodeId, Int>
    ): Map<NodeId, List<Shard>> {
        val moves = linkedMapOf<NodeId, MutableList<Shard>>()
        val surplusIterator = surplus.iterator()
        deficit.forEach { (targetNode, needed) ->
            repeat(needed) {
                if (surplusIterator.hasNext()) {
                    moves.computeIfAbsent(targetNode) { mutableListOf() }.add(surplusIterator.next())
                }
            }
        }
        return moves
    }

    companion object {
        fun check(command: Command.Check): ShardBalanceResult {
            val nodes = ClusterHelper.readNodes(command.nodes.dataSources)
            val (_, shards) = MoveHelpers.readShards(nodes)
            val knownNodes = nodes.keys.map { it.id }.toSet()
            return ShardBalance(shards.values, knownNodes).compute()
        }
    }
}
