package kolbasa.cluster.butcher.check

import kolbasa.schema.NodeId

internal data class OrphanTable(
    val companionTable: String,
    val missingMainTable: String,
)

internal data class OrphanTablesResult(
    val orphansByNode: Map<NodeId, List<OrphanTable>>,
) {

    val totalOrphans: Int = orphansByNode.values.sumOf { it.size }

    val isClean: Boolean = totalOrphans == 0
}
