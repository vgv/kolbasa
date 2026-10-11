package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.OrphanTablesResult

/**
 * Renders an [OrphanTablesResult] the way butcher shows it
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun OrphanTablesResult.render(): Widget = verticalLayout {
    if (isClean) {
        cell(TextColors.green("Orphan tables: no orphan queue tables"))
        return@verticalLayout
    }

    cell(TextColors.yellow("Orphan tables: $totalOrphans orphan queue(s) across ${orphansByNode.size} node(s)"))
    cell(table {
        header { row("Node", "Orphan table", "Missing main queue") }
        body {
            orphansByNode.toSortedMap().forEach { (node, orphans) ->
                orphans.sortedBy { it.companionTable }.forEach { orphan ->
                    row(node.id, orphan.companionTable, orphan.missingMainTable)
                }
            }
        }
    })
}
