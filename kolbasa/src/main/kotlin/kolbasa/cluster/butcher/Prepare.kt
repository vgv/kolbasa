package kolbasa.cluster.butcher

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.usePreparedStatement
import javax.sql.DataSource

internal fun prepare(
    dataSources: List<DataSource>,
    target: NodeId,
    shards: List<ShardId>,
    progressCallback: ProgressCallback
) {
    val nodes = ClusterHelper.readNodes(dataSources)
    val (shardDataSource, initialShards) = MoveHelpers.readShards(nodes)

    // Check that target node exists
    if (nodes.none { (node, _) -> node.id == target }) {
        throw ButcherException.MoveToNonExistingNodeException(nodes.keys.toList(), target)
    }

    // Check that we are not going to migrate shard to the same node where the shard is located right now
    initialShards
        .filterKeys { shard ->
            shard in shards
        }.forEach { (_, shard) ->
            if (shard.producerNode == target && shard.consumerNode == target) {
                throw ButcherException.MoveToTheSameShardException(shard.shard, target)
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
                ${ShardSchema.SHARD_COLUMN_NAME} in (${shards.joinToString(",") { it.id.toString() }})
        """.trimIndent()

    shardDataSource.usePreparedStatement(query) { ps ->
        ps.setString(1, target.id)
        ps.setString(2, target.id)
        ps.executeUpdate()
    }

    // Calculate difference between initial and updated shards and notify
    val updatedShards = ShardSchema.readShards(shardDataSource)
    progressCallback.prepareSuccessful(
        shards = shards,
        targetNode = target,
        shardsDiff = MoveHelpers.calculateShardsDiff(initialShards, updatedShards)
    )
}
