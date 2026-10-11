package kolbasa.cluster.butcher.check

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * How the shards are spread across the cluster right now, and how to even it out
 *
 * [proposedMoves] is a plan and nothing more – reading it changes nothing. For each node it names the shards
 * that should be handed over to it, chosen so that the total number of shards that have to move is as small
 * as possible. A node's list goes straight into
 * [Butcher.prepareMigration][kolbasa.butcher.Butcher.prepareMigration].
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

    /** How many shards the plan would move in total */
    val totalMoves: Int = proposedMoves.values.sumOf { it.size }

    /** True when nothing needs to move */
    val isBalanced: Boolean = totalMoves == 0
}
