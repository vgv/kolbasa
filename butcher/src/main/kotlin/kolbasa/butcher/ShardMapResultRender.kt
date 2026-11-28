package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextAlign
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.ShardMapResult
import kolbasa.producer.ShardId

/**
 * Renders a [ShardMapResult] the way butcher shows it
 *
 * With [advice] the report also says which command fixes what it found. The command that is about to fix it leaves it out.
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun ShardMapResult.render(advice: Boolean = true): Widget = verticalLayout {
    if (isClean) {
        cell(TextColors.green("Shard map: all ${copies.size} copies are the same"))
        return@verticalLayout
    }

    if (isMissing) {
        cell(TextColors.yellow("Shard map: no shard map on any node yet"))
        if (advice) {
            cell("An application creates it when it starts, and so does 'init-shards'.")
            cell("If applications already work with this cluster, check that the config points at the right databases.")
        }
    } else {
        cell(TextColors.yellow("Shard map: ${problems().joinToString(", ")}"))

        if (differentCopies > 0) {
            cell(copiesTable())
            if (advice) {
                cell("Applications sync the copies on their next topology update. With no application running, use")
                cell("'sync-shards'.")
            }
        }

        // Out of the table, as in the migration state report: a long list of shards leaves no room for the other columns
        if (shardsWithoutRow.isNotEmpty()) {
            cell("No row on any node: ${shardsWithoutRow.toText()}")
            if (advice) {
                cell("Applications give these shards a node on their next topology update. With no application running,")
                cell("use 'init-shards'.")
            }
        }
    }

    invalidShards.toSortedMap().forEach { (node, shards) ->
        cell("${node.id}: rows out of ${ShardId.MIN_SHARD}..${ShardId.MAX_SHARD}: ${shards.sorted().joinToString(",")}")
    }

    unknownOwners.toSortedMap().forEach { (owner, shards) ->
        cell("Owner ${owner.id} is not in the node list: shards ${shards.toText()}")
    }
}

private fun ShardMapResult.problems(): List<String> {
    val problems = mutableListOf<String>()

    if (differentCopies > 0) {
        problems += "$differentCopies of ${copies.size + nodesWithoutTable.size} copies differ"
    }
    if (shardsWithoutRow.isNotEmpty()) {
        problems += "${shardsWithoutRow.size} shard(s) have no row on any node"
    }
    if (invalidShards.isNotEmpty()) {
        problems += "${invalidShards.values.sumOf { it.size }} row(s) of shards that don't exist"
    }
    if (unknownOwners.isNotEmpty()) {
        problems += "${unknownOwners.size} owner(s) not in the node list"
    }

    return problems
}

private fun ShardMapResult.copiesTable(): Widget = table {
    header { row("Node", "Missing", "Outdated", "Conflicting") }
    column(1) { align = TextAlign.RIGHT }
    column(2) { align = TextAlign.RIGHT }
    column(3) { align = TextAlign.RIGHT }
    body {
        (copies.keys + nodesWithoutTable).sorted().forEach { node ->
            val copy = copies[node]
            if (copy == null) {
                row(node.id, "no table", "", "")
            } else {
                row(node.id, copy.missing.size, copy.outdated.size, copy.conflicting.size)
            }
        }
    }
}

private fun Collection<ShardId>.toText(): String =
    sorted().joinToString(",") { it.id.toString() }
