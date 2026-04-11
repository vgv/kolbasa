package kolbasa.cluster.butcher.check

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.butcher.config.Command
import kolbasa.schema.Const
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaExtractor
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import javax.sql.DataSource

/**
 * Find companion tables (`_dlq`, `_arc`) whose main queue table is missing on
 * the same node. Each node is inspected independently.
 */
internal class OrphanTables(
    private val tablesByNode: Map<NodeId, Set<String>>,
) {

    fun compute(): OrphanTablesResult {
        val orphansByNode = tablesByNode
            .mapValues { (_, tables) -> findOrphansOnNode(tables) }
            .filterValues { it.isNotEmpty() }

        return OrphanTablesResult(orphansByNode)
    }

    private fun findOrphansOnNode(tables: Set<String>): List<OrphanTable> =
        tables
            .mapNotNull { table ->
                val mainTableName = mainTableFor(table) ?: return@mapNotNull null
                if (mainTableName in tables)
                    null
                else
                    OrphanTable(table, mainTableName)
            }
            .sortedBy { it.companionTable }

    /**
     * Return the expected main table name for a companion, or null if [table]
     * is not a companion. Rejects degenerate names like `q__dlq` (no queue name
     * between prefix and suffix).
     */
    private fun mainTableFor(table: String): String? {
        if (table.startsWith(Const.INTERNAL_KOLBASA_TABLE_PREFIX))
            return null

        if (!table.startsWith(Const.QUEUE_TABLE_NAME_PREFIX))
            return null

        val suffix = when {
            table.endsWith(Const.DLQ_TABLE_NAME_SUFFIX) -> Const.DLQ_TABLE_NAME_SUFFIX
            table.endsWith(Const.ARCHIVE_TABLE_NAME_SUFFIX) -> Const.ARCHIVE_TABLE_NAME_SUFFIX
            else -> return null
        }

        val mainTable = table.removeSuffix(suffix)
        if (mainTable.length <= Const.QUEUE_TABLE_NAME_PREFIX.length) return null
        return mainTable
    }

    companion object {
        fun check(command: Command.Check): OrphanTablesResult {
            val nodes = ClusterHelper.readNodes(command.nodes.dataSources)

            val tablesByNode: ConcurrentMap<NodeId, Set<String>> = ConcurrentHashMap()
            // Collect all tables from all nodes
            nodes.entries.parallelStream().forEach { (node, dataSource: DataSource) ->
                tablesByNode[node.id] = SchemaExtractor.extractRawSchema(dataSource).keys.toSet()
            }

            return OrphanTables(tablesByNode).compute()
        }
    }
}
