package kolbasa.cluster.migrate

import kolbasa.cluster.migrate.utils.MigrateHelpers
import kolbasa.cluster.migrate.utils.MigrateHelpers.calculateShardsDiff
import kolbasa.cluster.schema.ShardSchema
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import javax.sql.DataSource
import kotlin.system.exitProcess

internal fun prepare(shards: List<Int>, targetNode: String, dataSources: List<DataSource>, events: MigrateEvents) {
    val nodes = MigrateHelpers.readNodes(dataSources)
    val (shardDataSource, initialShards) = MigrateHelpers.readShards(nodes)

    // Check that target node exists
    if (nodes.none { (node, _) -> node.serverId == targetNode }) {
        events.prepareMigrateToNonExistingNodeError(nodes.keys.toList(), targetNode)
        exitProcess(1)
    }

    // Check that we are not going to migrate shard to the same node where the shard is located right now
    initialShards
        .filterKeys { shard ->
            // First, find all shards that we are going to migrate
            shard in shards
        }.forEach { (_, shard) ->
            // Then, check that we are not going to migrate shard to the same node where the shard is located right now
            if (shard.producerNode == targetNode && shard.consumerNode == targetNode) {
                events.prepareMigrateToTheSameShardError(shard, targetNode)
                exitProcess(1)
            }
        }

    // Ok, everything looks fine, prepare shard table to migration
    val query = """
            update ${ShardSchema.SHARD_TABLE_NAME}
            set
                ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = ?,
                ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = null,
                ${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME} = ?
            where
                ${ShardSchema.SHARD_COLUMN_NAME} in (${shards.joinToString(",")})
        """.trimIndent()

    shardDataSource.usePreparedStatement(query) { ps ->
        ps.setString(1, targetNode)
        ps.setString(2, targetNode)
        ps.executeUpdate()
    }

    // Calculate difference between initial and updated shards and notify the caller
    val updatedShards = ShardSchema.readShards(shardDataSource)
    events.prepareSuccessful(
        shards = shards,
        targetNode = targetNode,
        shardsDiff = calculateShardsDiff(initialShards, updatedShards)
    )
}

