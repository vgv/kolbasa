package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render
import kolbasa.cluster.admin.ClusterOperationException
import kolbasa.schema.NodeId

internal class SyncShards : MutatingClusterCommand(name = "sync-shards") {

    override val helpSection = HelpSection.MAINTENANCE

    override fun help(context: Context) =
        "Bring the copies of the shard map on the nodes up to date: every copy gets the newest row of every shard. " +
            "Applications do the same on every topology update, so this is for a cluster with no application running"

    override fun execute(): Widget {
        val butcher = butcher()

        val shardMap = butcher.checkShardMap()
        if (shardMap.isMissing) {
            // Nothing to copy from, and the library refuses the same way
            throw ClusterOperationException.ShardsNotInitializedException()
        }
        if (shardMap.differentCopies == 0) {
            // Nothing to agree to, and the report says so itself
            return shardMap.render()
        }

        // The report goes on screen before the question, so what is being agreed to is visible. It is the
        // same report 'check-cluster --checks=shard-map' shows
        terminal.println(shardMap.render(advice = false))
        if (shardMap.shardsWithoutRow.isNotEmpty()) {
            terminal.println(
                "${shardMap.shardsWithoutRow.size} shard(s) have no row on any node. This command doesn't give them a " +
                    "node: that is 'init-shards'."
            )
        }
        val nodes = shardMap.copies.size + shardMap.nodesWithoutTable.size
        confirm("${shardMap.differentCopies} of $nodes copies will be brought up to date.")

        val changedRows = butcher.syncShards()

        // Where the copies ended up, read back rather than predicted
        val shardMapAfter = butcher.checkShardMap()
        return verticalLayout {
            changedRows.forEach { (node, rows) ->
                cell(describe(node, rows))
            }
            cell(shardMapAfter.render())
        }
    }

    private fun describe(node: NodeId, changedRows: Int): String {
        return if (changedRows == 0) {
            "${node.id}: up to date"
        } else {
            "${node.id}: $changedRows row(s) changed"
        }
    }
}
