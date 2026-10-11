package kolbasa.cluster.butcher.check

import kolbasa.butcher.ColumnShape
import kolbasa.schema.NodeId

/**
 * How one node's copy of a queue table differs from the shape the majority of nodes agree on
 */
sealed interface NodeDelta {

    /** The table is not on this node at all */
    object Missing : NodeDelta

    /**
     * The table is there but its shape is not the same
     *
     * A column present on both sides with a different type, nullability or default is reported once, in
     * [changedColumns], and not as an addition plus a removal.
     */
    data class ShapeDiff(
        val addedColumns: Set<ColumnShape>,
        val removedColumns: Set<ColumnShape>,
        val changedColumns: Set<ColumnChange>,
        val addedIndexes: Set<String>,
        val removedIndexes: Set<String>,
        val identityPresenceDiffers: Boolean,
    ) : NodeDelta
}

/**
 * One column that exists on both sides but does not look the same
 *
 * @property reference the column as the majority of nodes have it
 * @property other the column as the diverging node has it
 */
data class ColumnChange(val reference: ColumnShape, val other: ColumnShape) {
    val name: String get() = reference.name
}

/**
 * Everything that is wrong with one queue table across the cluster
 *
 * @property table the table name
 * @property referenceNodes the nodes that agree on the shape taken as correct – the majority, or the
 * alphabetically first group when there is no majority
 * @property deltasByNode what each of the remaining nodes does differently
 */
data class TableDivergence(
    val table: String,
    val referenceNodes: Set<NodeId>,
    val deltasByNode: Map<NodeId, NodeDelta>,
)

/**
 * Queue tables whose structure is not the same on every node, keyed by table name
 *
 * Divergence is the one thing that blocks a migration: moving rows between nodes needs identical tables, so
 * `move` refuses to run while a table differs. Values of identity columns are deliberately not compared –
 * those are per-node by design (see docs/Message IDs.md).
 */
data class SchemaConsistencyResult(
    val divergencesByTable: Map<String, TableDivergence>,
) {

    val totalDivergent: Int = divergencesByTable.size

    val isClean: Boolean = totalDivergent == 0
}
