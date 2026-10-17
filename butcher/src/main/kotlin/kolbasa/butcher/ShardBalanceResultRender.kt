package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextAlign
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.ShardBalanceResult
import kolbasa.schema.NodeId

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

    cell(distributionTable())

    if (!isBalanced) {
        cell(TextColors.yellow("Proposed moves ($totalMoves):"))
        proposedMoves.toSortedMap().forEach { (target, shards) ->
            cell("  ${target.id} receives ${shards.size} shard(s):")
            cell("    --target=${target.id} --shards=${shards.sorted().joinToString(",") { it.id.toString() }}")
        }
    }
}

/**
 * Renders the same report read as the plan to empty [drainedNodes]
 *
 * In a drain plan `isBalanced` means that [drainedNodes] own nothing, not that the cluster is even, and the
 * proposed moves are only their shards. A 0/512/512 cluster is not "evenly spread", so a drain gets its
 * own headline instead of the balance one.
 *
 * The shard numbers are left out here as well. There is no flag to paste them into: a drain is named by
 * its nodes, and which of their shards go where is the command's decision.
 */
internal fun ShardBalanceResult.renderNodesDrain(drainedNodes: Set<NodeId>): Widget = verticalLayout {
    val names = drainedNodes.sorted().joinToString(", ") { it.id }
    val stayingNodes = currentDistribution.size - drainedNodes.size

    cell(
        if (!isBalanced) {
            TextColors.yellow("Draining $names: $totalMoves shard(s) to move onto $stayingNodes node(s)")
        } else if (drainedNodes.size == 1) {
            TextColors.green(
                "Node $names owns no shards: nothing is produced to it and nothing is consumed from it"
            )
        } else {
            TextColors.green(
                "Nodes $names own no shards: nothing is produced to them and nothing is consumed from them"
            )
        }
    )

    cell(distributionTable())
}

/**
 * Who holds how many shards, and what the plan would do to that
 *
 * The `Change` column appears only when there is something to move. Otherwise it is a column of dashes.
 */
private fun ShardBalanceResult.distributionTable(): Widget = table {
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
}

/** `+14`, `-70`, or a dash when the plan leaves the node alone */
private fun formatChange(change: Int): String = when {
    change > 0 -> "+$change"
    change < 0 -> "$change"
    else -> "-"
}
