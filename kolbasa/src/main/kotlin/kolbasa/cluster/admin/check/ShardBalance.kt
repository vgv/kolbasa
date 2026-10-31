package kolbasa.cluster.admin.check

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.admin.ClusterOperationException
import kolbasa.cluster.admin.MoveHelpers
import kolbasa.cluster.admin.ShardBalanceResult
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

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
 *
 * A node in [drainedNodes] is being emptied. Its shards still count toward N, but its target is 0 and K
 * counts only the nodes that stay. Leaving the node out of [knownNodes] instead would skip its shards as
 * orphans, which is the opposite of a drain.
 *
 * While draining, only the drained nodes give shards away. Evening out the nodes that stay is a rebalance,
 * which is a separate request.
 */
internal class ShardBalance(
    private val shards: Collection<Shard>,
    private val knownNodes: Set<NodeId>,
    private val drainedNodes: Set<NodeId> = emptySet()
) {

    init {
        require(drainedNodes.all { it in knownNodes }) {
            "Drained nodes must be cluster members, unknown: ${(drainedNodes - knownNodes).map { it.id }}"
        }
        require(knownNodes.isEmpty() || drainedNodes.size < knownNodes.size) {
            "Cannot drain every node of the cluster, there would be nowhere to put the shards"
        }
    }

    /**
     * Known node -> shards it currently owns (producerNode). Orphan shards are excluded.
     */
    private val currentDistribution: Map<NodeId, List<Shard>> = groupShardsByNode()

    /**
     * Known node -> how many shards it should own after rebalance (floor or ceil of N/K, or 0 if drained).
     */
    private val targetShardsPerNode: Map<NodeId, Int> = computeTargetPerNode()

    fun compute(): ShardBalanceResult {
        if (knownNodes.isEmpty()) {
            return ShardBalanceResult(currentDistribution.toShardIds(), emptyMap())
        }

        val surplusShards = collectSurplusShards()
        val deficitByNode = collectDeficitByNode()
        val proposedMoves = assignSurplusToDeficit(surplusShards, deficitByNode)

        return ShardBalanceResult(currentDistribution.toShardIds(), proposedMoves.toShardIds())
    }

    /**
     * The result only names the shards: which node holds them is the key of the map it goes into, and the
     * full ownership record is an internal detail of the computation.
     */
    private fun Map<NodeId, List<Shard>>.toShardIds(): Map<NodeId, List<ShardId>> =
        mapValues { (_, shards) -> shards.map { it.shard } }

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
        // A drained node counts in the total but not in the divisor: 1024 shards over 3 nodes, one of
        // them drained, is 512/512/0.
        val totalBalancedShards = currentDistribution.values.sumOf { it.size }
        val receivingNodes = knownNodes - drainedNodes
        val floor = totalBalancedShards / receivingNodes.size
        val ceil = floor + 1
        val ceilSlots = totalBalancedShards % receivingNodes.size

        // Give the ceil target to the heaviest nodes. Keeping the shards that are already
        // on heavy nodes there minimizes how many shards have to move.
        // Sort heaviest-first; tie-break by NodeId for determinism.
        val rankedNodes = currentDistribution
            .entries
            .filterNot { it.key in drainedNodes }
            .sortedWith(compareByDescending<Map.Entry<NodeId, List<Shard>>> { it.value.size }.thenBy { it.key })
            .map { it.key }

        val result = mutableMapOf<NodeId, Int>()
        // A target of 0 makes everything a drained node holds surplus by the ordinary rule below
        drainedNodes.forEach { node ->
            result[node] = 0
        }
        rankedNodes.forEachIndexed { rank, node ->
            result[node] = if (rank < ceilSlots) ceil else floor
        }
        return result
    }

    /**
     * Shards that must leave their current (overloaded) node, ordered by shard number.
     *
     * While draining, only the nodes being emptied give shards away – see the class comment.
     */
    private fun collectSurplusShards(): List<Shard> {
        val surplus = mutableListOf<Shard>()
        val givingNodes = drainedNodes.ifEmpty { targetShardsPerNode.keys }
        givingNodes.forEach { node ->
            val wanted = targetShardsPerNode.getValue(node)
            val current = currentDistribution.getValue(node)
            if (current.size > wanted) {
                surplus += current.sortedBy { it.shard }.take(current.size - wanted)
            }
        }
        return surplus.sortedBy { it.shard }
    }

    /**
     * Node -> number of shards it still needs to receive to reach its target, neediest first.
     *
     * The order matters only while draining. A plain rebalance has exactly as much surplus as deficit, so
     * every node here is filled anyway. A drain can have less, and then the shards go where the hole is
     * deepest.
     */
    private fun collectDeficitByNode(): Map<NodeId, Int> {
        return targetShardsPerNode
            .mapValues { (node, wanted) -> wanted - currentDistribution.getValue(node).size }
            .filterValues { it > 0 }
            .entries
            .sortedWith(compareByDescending<Map.Entry<NodeId, Int>> { it.value }.thenBy { it.key })
            .associate { it.key to it.value }
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
        fun check(dataSources: List<DataSource>, drainedNodes: Set<NodeId> = emptySet()): ShardBalanceResult {
            val nodes = ClusterHelper.readNodes(dataSources)
            val (_, shards) = MoveHelpers.readShards(nodes)
            val knownNodes = nodes.keys.map { it.id }.toSet()

            // An operator types the node to drain by hand, so a typo gets a message rather than the
            // constructor's assertion
            val unknownNodes = drainedNodes - knownNodes
            if (unknownNodes.isNotEmpty()) {
                throw ClusterOperationException.DrainUnknownNodeException(knownNodes.sorted(), unknownNodes.sorted())
            }
            if (drainedNodes.isNotEmpty() && drainedNodes.size >= knownNodes.size) {
                throw ClusterOperationException.DrainLastNodeException(drainedNodes.sorted())
            }

            return ShardBalance(shards.values, knownNodes, drainedNodes).compute()
        }
    }
}
