package kolbasa.cluster.butcher.check

import kolbasa.cluster.Shard
import kolbasa.schema.NodeId

internal data class ShardBalanceResult(
    val currentDistribution: Map<NodeId, List<Shard>>,
    val proposedMoves: Map<NodeId, List<Shard>>,
) {

    val totalMoves: Int = proposedMoves.values.sumOf { it.size }

    val isBalanced: Boolean = totalMoves == 0
}
