package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.InitClusterPlan

/**
 * Renders an [InitClusterPlan] the way butcher shows it before the question
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun InitClusterPlan.render(): Widget = verticalLayout {
    if (isEmpty) {
        cell(TextColors.green("The cluster is initialized already"))
        return@verticalLayout
    }

    nodes.newNodes.forEach { node ->
        cell("${node.id.id}: gets its id, with bucket ${node.bucket}")
    }

    val shardsPerNode = shards.owners.values.groupingBy { node -> node }.eachCount().toSortedMap()
    shardsPerNode.forEach { (node, shards) ->
        cell("${node.id}: gets $shards shard(s)")
    }

    if (copiesToSync > 0) {
        val copies = nodes.newNodes.size + nodes.initializedNodes.size
        cell("$copiesToSync of $copies copies of the shard map will be brought up to date")
    }

    unsafeNodes.forEach { node ->
        // A node is unsafe for the shard map only while there is none, and then the way out is to start with it alone
        val warning = if (node in shards.unsafeNodes) {
            "${node.id} has messages, and the cluster has no shard map yet. Initializing it together with other nodes " +
                "is unsafe: the messages of the shards that go to other nodes stay unread. Initialize the cluster " +
                "with this node alone first, then add the other nodes."
        } else {
            "${node.id} has messages but no id yet. Adding it to this cluster is unsafe: its messages stay unread, as " +
                "its shards belong to other nodes."
        }
        cell(TextColors.yellow(warning))
    }
}
