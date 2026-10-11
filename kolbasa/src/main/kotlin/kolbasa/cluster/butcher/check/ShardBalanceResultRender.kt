package kolbasa.cluster.butcher.check

import kolbasa.butcher.ShardBalanceResult

internal fun ShardBalanceResult.render(): String = buildString {
    val sortedNodes = currentDistribution.keys.sorted()
    val width = sortedNodes.maxOfOrNull { it.id.length } ?: 0

    appendLine("Shard balance:")
    appendLine("  Current distribution (${currentDistribution.values.sumOf { it.size }} shards across ${sortedNodes.size} nodes):")
    sortedNodes.forEach { node ->
        val shards = currentDistribution[node] ?: emptyList()
        val shardsCount = shards.size
        val allShardsString = shards.joinToString(separator = ", ", prefix = "[", postfix = "]") { it.id.toString() }
        appendLine("    ${node.id.padEnd(width)}  $shardsCount shards $allShardsString")
    }

    if (isBalanced) {
        append("  Already balanced, no moves needed.")
        return@buildString
    }

    appendLine("  Proposed moves ($totalMoves):")
    proposedMoves.toSortedMap().forEach { (target, shards) ->
        val sorted = shards.sorted()
        appendLine("    ⟶ ${target.id} (${sorted.size} shards):")
        appendLine("      shards: ${sorted.joinToString(separator = ",") { it.id.toString() }}")
        appendLine("      target: ${target.id}")
    }
}.trimEnd()
