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
            val shards = currentDistribution[node] ?: emptyList()
            val shardsCount = shards.size
            val allShardsString = shards.joinToString(separator = ", ", prefix = "[", postfix = "]") { it.shard.toString() }
            appendLine("    ${node.id.padEnd(width)}  $shardsCount shards $allShardsString")
        }

        if (isBalanced) {
            append("  Already balanced, no moves needed.")
            return@buildString
        }

        appendLine("  Proposed moves ($totalMoves):")
        proposedMoves.toSortedMap().forEach { (target, shards) ->
            val sorted = shards.sortedBy { it.shard }
            appendLine("    ⟶ ${target.id} (${sorted.size} shards):")
            appendLine("      shards: ${sorted.joinToString(separator = ",") { it.shard.toString() }}")
            appendLine("      target: ${target.id}")
        }
    }.trimEnd()
}
