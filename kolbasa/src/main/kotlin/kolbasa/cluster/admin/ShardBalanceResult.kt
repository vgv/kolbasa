package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * How the shards are spread across the cluster right now, and how to even it out
 *
 * [proposedMoves] is a plan and nothing more – reading it changes nothing. For each node it names the shards
 * that should be handed over to it, chosen so that the total number of shards that have to move is as small
 * as possible. A node's list goes straight into
 * [ClusterAdmin.prepareMigration][kolbasa.cluster.admin.ClusterAdmin.prepareMigration].
 *
 * Where a proposed shard comes from is not repeated here: [currentDistribution] already says which node
 * holds every shard.
 *
 * A shard whose node is not part of the cluster any more is counted nowhere and proposed nowhere – such a
 * shard is a problem of its own, and [SchemaConsistencyResult] and [MigrationStateResult] describe it better.
 */
data class ShardBalanceResult(
    val currentDistribution: Map<NodeId, List<ShardId>>,
    val proposedMoves: Map<NodeId, List<ShardId>>,
) {

    /**
     * How many shards the plan would move in total
     */
    val totalMoves: Int = proposedMoves.values.sumOf { it.size }

    /**
     * True when nothing needs to move
     */
    val isBalanced: Boolean = totalMoves == 0

    /**
     * Node -> how many shards it gains or loses if [proposedMoves] is applied
     *
     * Positive means the node takes shards over, negative means it hands them away, and zero means the plan
     * leaves it as it is. Every node of [currentDistribution] is in the map, and the values add up to zero –
     * a move always has two sides.
     *
     * The giving side is not stated by [proposedMoves], which only says where each shard goes, so it is
     * derived here from [currentDistribution]: whoever holds a shard now is the node that loses it.
     */
    val netChangeByNode: Map<NodeId, Int> = run {
        val currentOwner = currentDistribution
            .flatMap { (node, shards) -> shards.map { shard -> shard to node } }
            .toMap()

        val outgoing = proposedMoves.values
            .flatten()
            .mapNotNull { currentOwner[it] }
            .groupingBy { it }
            .eachCount()

        val nodes = currentDistribution.keys + proposedMoves.keys
        nodes.associateWith { node ->
            (proposedMoves[node]?.size ?: 0) - (outgoing[node] ?: 0)
        }
    }
}
