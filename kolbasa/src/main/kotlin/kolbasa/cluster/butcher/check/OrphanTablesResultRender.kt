package kolbasa.cluster.butcher.check

import kolbasa.butcher.OrphanTablesResult

internal fun OrphanTablesResult.render(): String = buildString {
    if (isClean) {
        append("Orphan tables: no orphan queue tables")
        return@buildString
    }

    appendLine("Orphan tables: $totalOrphans orphan queue(s) across ${orphansByNode.size} node(s)")
    orphansByNode.toSortedMap().forEach { (node, orphans) ->
        appendLine("  ${node.id}:")
        val width = orphans.maxOf { it.companionTable.length }
        orphans.sortedBy { it.companionTable }.forEach { orphan ->
            appendLine("    ${orphan.companionTable.padEnd(width)}  (main queue ${orphan.missingMainTable} missing)")
        }
    }
}.trimEnd()
