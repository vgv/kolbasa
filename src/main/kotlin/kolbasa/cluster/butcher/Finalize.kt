package kolbasa.cluster.butcher

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.butcher.config.Command
import kolbasa.cluster.schema.ShardSchema
import kolbasa.utils.JdbcHelpers.useStatement

internal fun finalize(command: Command.Finalize) {
    val nodes = ClusterHelper.readNodes(command.nodes.dataSources)
    val (shardDataSource, initialShards) = MoveHelpers.readShards(nodes)

    // Okay, everything looks good, let's switch the shard table to a stable state
    val query = """
            update ${ShardSchema.SHARD_TABLE_NAME}
            set
                ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = ${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME},
                ${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME} = null
            where
                ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} is null
        """.trimIndent()

    shardDataSource.useStatement { statement ->
        statement.executeUpdate(query)
    }

    // Calculate difference between initial and updated shards and notify
    val updatedShards = ShardSchema.readShards(shardDataSource)
    ConsoleProgressCallback.finalizeSuccessful(
        shardsDiff = MoveHelpers.calculateShardsDiff(initialShards, updatedShards)
    )
}
