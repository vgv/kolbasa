package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextAlign
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.ShardBalanceResult

/**
 * Renders a [ShardBalanceResult] the way butcher shows it
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 *
 * Shard numbers are never put in a table cell. A node can hold hundreds of them, and a cell that long
 * squeezes every other column down to nothing; the proposed moves print theirs as the `--shards` flag they
 * are about to be pasted into instead.
 */
internal fun ShardBalanceResult.render(): Widget = verticalLayout {
    val totalShards = currentDistribution.values.sumOf { it.size }

    if (currentDistribution.isEmpty()) {
        cell(TextColors.yellow("Shard balance: no shard belongs to a node of this cluster"))
        return@verticalLayout
    }

    cell(
        if (isBalanced) {
            TextColors.green("Shard balance: $totalShards shard(s) evenly spread across ${currentDistribution.size} node(s)")
        } else {
            TextColors.yellow(
                "Shard balance: $totalShards shard(s) across ${currentDistribution.size} node(s), " +
                    "$totalMoves should move"
            )
        }
    )

    cell(table {
        header { if (isBalanced) row("Node", "Shards") else row("Node", "Shards", "Change") }
        column(1) { align = TextAlign.RIGHT }
        column(2) { align = TextAlign.RIGHT }
        body {
            currentDistribution.keys.sorted().forEach { node ->
                val shards = currentDistribution.getValue(node).size
                if (isBalanced) {
                    row(node.id, shards)
                } else {
                    row(node.id, shards, formatChange(netChangeByNode[node] ?: 0))
                }
            }
        }
    })

    if (!isBalanced) {
        cell(TextColors.yellow("Proposed moves ($totalMoves):"))
        proposedMoves.toSortedMap().forEach { (target, shards) ->
            cell("  ${target.id} receives ${shards.size} shard(s):")
            cell("    --target=${target.id} --shards=${shards.sorted().joinToString(",") { it.id.toString() }}")
        }
    }
}

/** `+14`, `-70`, or a dash when the plan leaves the node alone */
private fun formatChange(change: Int): String = when {
    change > 0 -> "+$change"
    change < 0 -> "$change"
    else -> "-"
}
