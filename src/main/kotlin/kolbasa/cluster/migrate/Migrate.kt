package kolbasa.cluster.migrate

import kolbasa.cluster.ClusterHelper
import kolbasa.schema.Node
import kolbasa.schema.SchemaExtractor
import kolbasa.schema.Table
import java.util.SortedMap
import javax.sql.DataSource
import kotlin.collections.component1
import kotlin.collections.component2

internal fun migrate(tablesToFind: Set<String>?, dataSources: List<DataSource>, events: MigrateEvents) {
    val nodes = ClusterHelper.readNodes(dataSources)

    // tablename => schema
    val schemas = findAndCompareAllSchemas(dataSources, tablesToFind)

    // targetnode => shards (which should be migrated to this node)
    val targetsToShards = findTargetNodeAndShards(nodes)

    // start move
    targetsToShards.forEach { (targetNode, shards) ->
        val (sources, target) = MigrateHelpers.splitNodes(nodes, targetNode)

        schemas.forEach { (_, schema) ->
            do {
                val migratedRows = sources.sumOf { sourceDS ->
                    val migrateOneTable = MigrateOneTable(
                        shards = shards,
                        schema = schema,
                        sourceDataSource = sourceDS,
                        targetDataSource = target,
                        rowsPerBatch = 1000,
                        moveProgressCallback = events
                    )
                    migrateOneTable.migrate()
                }
            } while (migratedRows > 0)
        }
    }
}

private fun findTargetNodeAndShards(nodes: SortedMap<Node, DataSource>): Map<String, List<Int>> {
    val (_, shards) = MigrateHelpers.readShards(nodes)

    val targetsToShards = mutableMapOf<String, MutableList<Int>>()
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
                    throw MigrateException.InconsistentSchemaException(tableName, firstTable.second, secondTable.second)
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
