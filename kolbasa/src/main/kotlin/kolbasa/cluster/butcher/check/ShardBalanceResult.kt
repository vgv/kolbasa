package kolbasa.cluster.butcher.check

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

internal data class ShardBalanceResult(
    val currentDistribution: Map<NodeId, List<ShardId>>,
    val proposedMoves: Map<NodeId, List<ShardId>>,
) {

    val totalMoves: Int = proposedMoves.values.sumOf { it.size }

    val isBalanced: Boolean = totalMoves == 0
}
