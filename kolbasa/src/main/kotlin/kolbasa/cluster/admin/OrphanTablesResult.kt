package kolbasa.cluster.admin

import kolbasa.schema.NodeId

/**
 * A companion table whose main queue table is missing from the same node
 *
 * @property companionTable the DLQ or archive table that is still there
 * @property missingMainTable the queue table it belongs to, which is not
 */
data class OrphanTable(
    val companionTable: String,
    val missingMainTable: String,
)

/**
 * Companion tables left behind without their main queue table, grouped by the node they sit on
 *
 * Usually the remains of a queue that was dropped on one node and not on the others. Nothing reads them, so
 * they are harmless until somebody recreates the queue and is surprised by what is already in its DLQ.
 */
data class OrphanTablesResult(
    val orphansByNode: Map<NodeId, List<OrphanTable>>,
) {

    /**
     * How many orphan tables were found across the whole cluster
     */
    val totalOrphans: Int = orphansByNode.values.sumOf { it.size }

    /**
     * True when no orphan table was found
     */
    val isClean: Boolean = totalOrphans == 0
}
