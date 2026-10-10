package kolbasa.cluster.butcher.check

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.butcher.config.Command
import kolbasa.schema.Column
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaExtractor
import kolbasa.schema.Table
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

/**
 * Detect cross-node divergence in kolbasa table schemas. Every kolbasa table that
 * exists anywhere in the cluster should exist on every node with the same shape
 * (columns, indexes, identity presence). Identity field values are deliberately
 * partitioned per node to avoid cluster-wide ID collisions, so only identity
 * presence is compared.
 */
internal class SchemaConsistency(
    private val schemasByNode: Map<NodeId, Map<String, Table>>,
) {

    fun compute(): SchemaConsistencyResult {
        val kolbasaTables = collectKolbasaTableNames()
        val divergences = kolbasaTables
            .mapNotNull { name -> analyzeTable(name)?.let { name to it } }
            .toMap()
            .toSortedMap()
        return SchemaConsistencyResult(divergences)
    }

    private fun collectKolbasaTableNames(): Set<String> = schemasByNode.values
        .flatMap { schema -> schema.values }
        .filter { it.isQueueTable() }
        .map { it.name }
        .toSet()

    private fun analyzeTable(name: String): TableDivergence? {
        val present = mutableMapOf<NodeId, Table>()
        val missing = mutableSetOf<NodeId>()
        schemasByNode.forEach { (node, schema) ->
            val table = schema[name]
            if (table != null)
                present[node] = table
            else
                missing += node
        }

        if (present.isEmpty()) return null

        val buckets = present.entries.groupBy({ it.value.fingerprint() }, { it.key })

        if (buckets.size == 1 && missing.isEmpty()) return null

        val referenceNodes = pickReferenceBucket(buckets)
        val referenceTable = present.getValue(referenceNodes.first())

        val deltas = sortedMapOf<NodeId, NodeDelta>()
        missing.forEach { deltas[it] = NodeDelta.Missing }
        present.forEach { (node, table) ->
            if (node !in referenceNodes) deltas[node] = diff(referenceTable, table)
        }

        return TableDivergence(name, referenceNodes, deltas)
    }

    /**
     * Pick the bucket with the most nodes; tiebreaker is the bucket containing the
     * lowest [NodeId]. Caller guarantees [buckets] is non-empty (so [reduce] is safe).
     */
    private fun pickReferenceBucket(buckets: Map<TableFingerprint, List<NodeId>>): Set<NodeId> =
        buckets.values.reduce { best, candidate ->
            when {
                candidate.size > best.size -> candidate
                candidate.size < best.size -> best
                candidate.min() < best.min() -> candidate
                else -> best
            }
        }.toSet()

    private fun diff(reference: Table, other: Table): NodeDelta.ShapeDiff {
        val rawAdded = other.columns - reference.columns
        val rawRemoved = reference.columns - other.columns
        val refByName = rawRemoved.associateBy { it.name }
        val otherByName = rawAdded.associateBy { it.name }
        val changedNames = refByName.keys intersect otherByName.keys

        val changed = changedNames
            .map { ColumnChange(reference = refByName.getValue(it), other = otherByName.getValue(it)) }
            .toSet()

        return NodeDelta.ShapeDiff(
            addedColumns = rawAdded.filterNot { it.name in changedNames }.toSet(),
            removedColumns = rawRemoved.filterNot { it.name in changedNames }.toSet(),
            changedColumns = changed,
            addedIndexes = other.indexes - reference.indexes,
            removedIndexes = reference.indexes - other.indexes,
            identityPresenceDiffers = (reference.identity == null) != (other.identity == null),
        )
    }

    private fun Table.fingerprint() = TableFingerprint(columns, indexes, identity != null)

    companion object {
        fun check(command: Command.Check): SchemaConsistencyResult {
            val nodes = ClusterHelper.readNodes(command.nodes.dataSources)

            val schemasByNode = ConcurrentHashMap<NodeId, Map<String, Table>>()
            nodes.entries.parallelStream().forEach { (node, dataSource: DataSource) ->
                schemasByNode[node.id] = SchemaExtractor.extractRawSchema(dataSource)
            }

            return SchemaConsistency(schemasByNode).compute()
        }
    }
}

private data class TableFingerprint(
    val columns: Set<Column>,
    val indexes: Set<String>,
    val hasIdentity: Boolean,
)
