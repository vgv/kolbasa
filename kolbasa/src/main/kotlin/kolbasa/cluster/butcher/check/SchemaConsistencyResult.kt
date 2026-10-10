package kolbasa.cluster.butcher.check

import kolbasa.schema.Column
import kolbasa.schema.NodeId

internal sealed interface NodeDelta {
    object Missing : NodeDelta

    data class ShapeDiff(
        val addedColumns: Set<Column>,
        val removedColumns: Set<Column>,
        val changedColumns: Set<ColumnChange>,
        val addedIndexes: Set<String>,
        val removedIndexes: Set<String>,
        val identityPresenceDiffers: Boolean,
    ) : NodeDelta
}

internal data class ColumnChange(val reference: Column, val other: Column) {
    val name: String get() = reference.name
}

internal data class TableDivergence(
    val table: String,
    val referenceNodes: Set<NodeId>,
    val deltasByNode: Map<NodeId, NodeDelta>,
)

internal data class SchemaConsistencyResult(
    val divergencesByTable: Map<String, TableDivergence>,
) {

    val totalDivergent: Int = divergencesByTable.size

    val isClean: Boolean = totalDivergent == 0

    override fun toString(): String = buildString {
        if (isClean) {
            append("Schema consistency: all tables consistent across cluster")
            return@buildString
        }

        appendLine("Schema consistency: $totalDivergent table(s) inconsistent across cluster")
        appendLine()
        divergencesByTable.toSortedMap().forEach { (_, divergence) ->
            appendDivergence(divergence)
        }
    }.trimEnd()

    private fun StringBuilder.appendDivergence(divergence: TableDivergence) {
        val refList = divergence.referenceNodes.sorted().joinToString(", ") { it.id }
        appendLine("  ${divergence.table} (reference: shape on [$refList]):")
        divergence.deltasByNode.toSortedMap().forEach { (node, delta) ->
            appendLine("    ${node.id}: ${formatDelta(delta)}")
        }
        appendLine()
    }

    private fun formatDelta(delta: NodeDelta): String = when (delta) {
        NodeDelta.Missing -> "missing"
        is NodeDelta.ShapeDiff -> formatShapeDiff(delta)
    }

    private fun formatShapeDiff(diff: NodeDelta.ShapeDiff): String {
        val parts = buildList {
            diff.changedColumns.sortedBy { it.name }.forEach {
                add("~column ${it.name} ${describe(it.reference)} -> ${describe(it.other)}")
            }
            diff.addedColumns.sortedBy { it.name }.forEach { add("+column ${it.name} ${describe(it)}") }
            diff.removedColumns.sortedBy { it.name }.forEach { add("-column ${it.name} ${describe(it)}") }
            diff.addedIndexes.sorted().forEach { add("+index $it") }
            diff.removedIndexes.sorted().forEach { add("-index $it") }
            if (diff.identityPresenceDiffers) add("identity presence differs")
        }
        return parts.joinToString(", ")
    }

    private fun describe(c: Column): String = buildString {
        append(c.type)
        append(if (c.nullable) " NULL" else " NOT NULL")
        c.defaultExpression?.let { append(" default=$it") }
    }
}
