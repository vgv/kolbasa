package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render
import kolbasa.cluster.admin.ShardMapResult
import kolbasa.schema.NodeId

internal class CreateOrRepairShardMap : MutatingClusterCommand(name = "create-or-repair-shard-map") {

    override val helpSection = HelpSection.SETUP

    override fun help(context: Context) =
        "Create the shard map of a new cluster, or bring its copies on the nodes up to date. Applications do the same on " +
            "every topology update, so this is for a cluster with no application running"

    override fun execute(): Widget {
        val butcher = butcher()

        val shardMap = butcher.checkShardMap()
        if (shardMap.isComplete && shardMap.differentCopies == 0) {
            // Nothing to agree to, and the report says so itself
            return shardMap.render(advice = false)
        }

        // The report goes on screen before the question, so what is being agreed to is visible. It is the
        // same report 'check-cluster --checks=shard-map' shows
        terminal.println(shardMap.render(advice = false))
        confirm(describe(shardMap))

        val changedRows = butcher.createOrRepairShardMap()

        // Where the copies ended up, read back rather than predicted
        val shardMapAfter = butcher.checkShardMap()
        return verticalLayout {
            changedRows.forEach { (node, rows) ->
                cell(describe(node, rows))
            }
            cell(shardMapAfter.render(advice = false))
        }
    }

    private fun describe(shardMap: ShardMapResult): String {
        val nodes = shardMap.copies.size + shardMap.nodesWithoutTable.size

        if (shardMap.isMissing) {
            return "There is no shard map on any node. A new one will be created on $nodes node(s), and every shard will get " +
                "a random node. If applications already work with this cluster, check that the config points at the right " +
                "databases."
        }

        val sentences = mutableListOf<String>()
        if (shardMap.shardsWithoutRow.isNotEmpty()) {
            sentences += "${shardMap.shardsWithoutRow.size} shard(s) have no row on any node, each will get a random node."
        }
        if (shardMap.differentCopies > 0) {
            sentences += "${shardMap.differentCopies} of $nodes copies will be brought up to date."
        }

        return sentences.joinToString(" ")
    }

    private fun describe(node: NodeId, changedRows: Int): String {
        return if (changedRows == 0) {
            "${node.id}: up to date"
        } else {
            "${node.id}: $changedRows row(s) changed"
        }
    }
}
