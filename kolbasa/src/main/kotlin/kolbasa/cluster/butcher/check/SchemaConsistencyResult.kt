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
}
