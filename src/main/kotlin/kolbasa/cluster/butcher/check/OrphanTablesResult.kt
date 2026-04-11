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

    override fun toString(): String = buildString {
        if (isClean) {
            append("Orphan tables: no orphan companion tables")
            return@buildString
        }

        appendLine("Orphan tables: $totalOrphans orphan companion(s) across ${orphansByNode.size} node(s)")
        orphansByNode.toSortedMap().forEach { (node, orphans) ->
            appendLine("  ${node.id}:")
            val width = orphans.maxOf { it.companionTable.length }
            orphans.sortedBy { it.companionTable }.forEach { orphan ->
                appendLine("    ${orphan.companionTable.padEnd(width)}  (main ${orphan.missingMainTable} missing)")
            }
        }
    }.trimEnd()
}
