package kolbasa.cluster.butcher.check

import kolbasa.cluster.Shard
import kolbasa.schema.NodeId

internal data class ShardBalanceResult(
    val currentDistribution: Map<NodeId, List<Shard>>,
    val proposedMoves: Map<NodeId, List<Shard>>,
) {

    val totalMoves: Int = proposedMoves.values.sumOf { it.size }

    val isBalanced: Boolean = totalMoves == 0

    override fun toString(): String = buildString {
        val sortedNodes = currentDistribution.keys.sorted()
        val width = sortedNodes.maxOfOrNull { it.id.length } ?: 0

        appendLine("Shard balance:")
        appendLine("  Current distribution (${currentDistribution.values.sumOf { it.size }} shards across ${sortedNodes.size} nodes):")
        sortedNodes.forEach { node ->
            val count = currentDistribution[node]?.size ?: 0
            appendLine("    ${node.id.padEnd(width)}  $count shards")
        }

        if (isBalanced) {
            append("  Already balanced, no moves needed.")
            return@buildString
        }

        appendLine("  Proposed moves ($totalMoves):")
        proposedMoves.toSortedMap().forEach { (target, shards) ->
            val sorted = shards.sortedBy { it.shard }
            appendLine("    -> ${target.id} (${sorted.size} shards):")
            sorted.forEach { shard ->
                appendLine("      shard ${shard.shard.toString().padStart(4)}: ${shard.producerNode.id} -> ${target.id}")
            }
        }
    }.trimEnd()
}
