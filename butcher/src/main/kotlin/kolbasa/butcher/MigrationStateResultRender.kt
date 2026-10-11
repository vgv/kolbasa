package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextAlign
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.MigrationStateResult

/**
 * Renders a [MigrationStateResult] the way butcher shows it
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun MigrationStateResult.render(): Widget = verticalLayout {
    if (isClean) {
        cell(TextColors.green("Migration state: no shards in migration"))
        return@verticalLayout
    }

    cell(TextColors.yellow("Migration state: $totalMigratingShards shard(s) in migration"))
    cell(table {
        header { row("Moving to", "Shards") }
        column(1) { align = TextAlign.RIGHT }
        body {
            migratingShardsByTarget.toSortedMap().forEach { (target, shards) ->
                row(target.id, shards.size)
            }
        }
    })

    // Out of the table for the same reason as in the balance report: a list of a few hundred shard numbers
    // in a cell leaves no room for the columns next to it
    migratingShardsByTarget.toSortedMap().forEach { (target, shards) ->
        cell("  ${target.id}: ${shards.sorted().joinToString(",") { it.id.toString() }}")
    }
}
