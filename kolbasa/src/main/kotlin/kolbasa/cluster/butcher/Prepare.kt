package kolbasa.cluster.butcher

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.butcher.config.Command
import kolbasa.cluster.schema.ShardSchema
import kolbasa.utils.JdbcHelpers.usePreparedStatement

internal fun prepare(command: Command.Prepare) {
    val nodes = ClusterHelper.readNodes(command.nodes.dataSources)
    val (shardDataSource, initialShards) = MoveHelpers.readShards(nodes)

    // Check that target node exists
    if (nodes.none { (node, _) -> node.id == command.target }) {
        throw ButcherException.MoveToNonExistingNodeException(nodes.keys.toList(), command.target)
    }

    // Check that we are not going to migrate shard to the same node where the shard is located right now
    initialShards
        .filterKeys { shard ->
            shard in command.shards
        }.forEach { (_, shard) ->
            if (shard.producerNode == command.target && shard.consumerNode == command.target) {
                throw ButcherException.MoveToTheSameShardException(shard, command.target)
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
                ${ShardSchema.SHARD_COLUMN_NAME} in (${command.shards.joinToString(",")})
        """.trimIndent()

    shardDataSource.usePreparedStatement(query) { ps ->
        ps.setString(1, command.target.id)
        ps.setString(2, command.target.id)
        ps.executeUpdate()
    }

    // Calculate difference between initial and updated shards and notify
    val updatedShards = ShardSchema.readShards(shardDataSource)
    ConsoleProgressCallback.prepareSuccessful(
        shards = command.shards,
        targetNode = command.target,
        shardsDiff = MoveHelpers.calculateShardsDiff(initialShards, updatedShards)
    )
}
