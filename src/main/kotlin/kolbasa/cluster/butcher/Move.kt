package kolbasa.cluster.butcher

import kolbasa.cluster.ClusterHelper
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaExtractor
import kolbasa.schema.Table
import java.util.SortedMap
import javax.sql.DataSource

/**
 * Transfer data from source nodes to target nodes for all shards in migration state.
 * Can be re-run safely if interrupted — INSERT uses ON CONFLICT DO NOTHING,
 * so duplicate rows are silently skipped on retry.
 */
internal fun move(command: Command.Move) {
    val nodes = ClusterHelper.readNodes(command.nodes.dataSources)

    // tablename => schema
    val schemas = findAndCompareAllSchemas(command.nodes.dataSources, command.tables)

    // targetnode => shards (which should be migrated to this node)
    val targetsToShards = findTargetNodeAndShards(nodes)

    // start move
    targetsToShards.forEach { (targetNode, shards) ->
        val (sources, target) = MoveHelpers.splitNodes(nodes, targetNode)

        schemas.forEach { (_, schema) ->
            do {
                val migratedRows = sources.sumOf { sourceDS ->
                    val moveOneTable = MoveOneTable(
                        shards = shards,
                        schema = schema,
                        sourceDataSource = sourceDS,
                        targetDataSource = target,
                        rowsPerBatch = 1000,
                        moveProgressCallback = ConsoleProgressCallback
                    )
                    moveOneTable.move()
                }
            } while (migratedRows > 0)
        }
    }
}

private fun findTargetNodeAndShards(nodes: SortedMap<Node, DataSource>): Map<NodeId, List<Int>> {
    val (_, shards) = MoveHelpers.readShards(nodes)

    val targetsToShards = mutableMapOf<NodeId, MutableList<Int>>()
    shards.values.forEach { shard ->
        if (shard.nextConsumerNode != null) {
            targetsToShards.computeIfAbsent(shard.nextConsumerNode) { mutableListOf() }.add(shard.shard)
        }
    }

    return targetsToShards
}

private fun findAndCompareAllSchemas(
    dataSources: List<DataSource>,
    tablesToFind: Set<String>?
): Map<String, Table> {
    // schemas
    val allSchemas = mutableMapOf<String, MutableList<Pair<DataSource, Table>>>()
    dataSources.forEach { dataSource ->
        SchemaExtractor.extractRawSchema(dataSource, tablesToFind).forEach { (tableName, table) ->
            allSchemas.computeIfAbsent(tableName) { mutableListOf() }.add(dataSource to table)
        }
    }

    // check that all schemas are the same
    allSchemas.forEach { (tableName, tables) ->
        for (i in 0..tables.size - 2) {
            val firstTable = tables[i]
            for (j in i + 1..tables.size - 1) {
                val secondTable = tables[j]
                if (!partialEq(firstTable.second, secondTable.second)) {
                    throw ButcherException.InconsistentSchemaException(tableName, firstTable.second, secondTable.second)
                }
            }
        }
    }

    return allSchemas.mapValues { it.value.first().second }
}


private fun partialEq(first: Table, second: Table): Boolean {
    val name = first.name == second.name
    if (!name) {
        return false
    }

    val columns = first.columns.size == second.columns.size
    if (!columns) {
        return false
    }

    return first.indexes.size == second.indexes.size
}
