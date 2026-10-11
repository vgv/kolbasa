package kolbasa.butcher.cli

import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.ColumnShape
import kolbasa.butcher.NodeDelta
import kolbasa.butcher.SchemaConsistencyResult
import kolbasa.butcher.TableDivergence

/**
 * Renders a [SchemaConsistencyResult] the way butcher shows it
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun SchemaConsistencyResult.render(): Widget = verticalLayout {
    if (isClean) {
        cell(TextColors.green("Schema consistency: all tables consistent across cluster"))
        return@verticalLayout
    }

    cell(TextColors.yellow("Schema consistency: $totalDivergent table(s) inconsistent across cluster"))
    divergencesByTable.toSortedMap().forEach { (_, divergence) ->
        cell(divergence.render())
    }
}

private fun TableDivergence.render(): Widget {
    val reference = referenceNodes.sorted().joinToString(", ") { it.id }

    return table {
        captionTop("$table (reference: shape on [$reference])")
        header { row("Node", "Difference") }
        body {
            deltasByNode.toSortedMap().forEach { (node, delta) ->
                row(node.id, delta.describe())
            }
        }
    }
}

private fun NodeDelta.describe(): String = when (this) {
    NodeDelta.Missing -> "missing"
    is NodeDelta.ShapeDiff -> describe()
}

private fun NodeDelta.ShapeDiff.describe(): String {
    val parts = buildList {
        changedColumns.sortedBy { it.name }.forEach {
            add("~column ${it.name} ${it.reference.describe()} -> ${it.other.describe()}")
        }
        addedColumns.sortedBy { it.name }.forEach { add("+column ${it.name} ${it.describe()}") }
        removedColumns.sortedBy { it.name }.forEach { add("-column ${it.name} ${it.describe()}") }
        addedIndexes.sorted().forEach { add("+index $it") }
        removedIndexes.sorted().forEach { add("-index $it") }
        if (identityPresenceDiffers) add("identity presence differs")
    }

    return parts.joinToString("\n")
}

private fun ColumnShape.describe(): String = buildString {
    append(type)
    append(if (nullable) " NULL" else " NOT NULL")
    defaultExpression?.let { append(" default=$it") }
}
