package kolbasa.cluster.admin

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

internal fun prepare(
    dataSources: List<DataSource>,
    target: NodeId,
    shards: List<ShardId>,
    progressCallback: ProgressCallback
) {
    val nodes = ClusterHelper.readNodes(dataSources)
    val shardMap = MoveHelpers.readShards(nodes)
    val initialShards = shardMap.mergedShards

    // Check that target node exists
    if (nodes.none { (node, _) -> node.id == target }) {
        throw ClusterOperationException.MoveToNonExistingNodeException(nodes.keys.map { it.id }, target)
    }

    // Check that we are not going to migrate shard to the same node where the shard is located right now
    initialShards
        .filterKeys { shard ->
            shard in shards
        }.forEach { (_, shard) ->
            if (shard.producerNode == target && shard.consumerNode == target) {
                throw ClusterOperationException.MoveToTheSameShardException(shard.shard, target)
            }
        }

    // Ok, everything looks fine, prepare the shards to migration
    val revision = ShardSchema.nextRevision(initialShards.values)
    val newRows = mutableListOf<Shard>()
    for (shardId in shards) {
        val newRow = Shard(
            shard = shardId,
            producerNode = target,
            consumerNode = null,
            nextConsumerNode = target,
            revision = revision
        )

        // The shard may be migrating to this node already
        if (!newRow.equalsIgnoreRevision(initialShards.getValue(shardId))) {
            newRows += newRow
        }
    }
    MoveHelpers.writeShards(nodes, shardMap, newRows)

    // Calculate difference between initial and updated shards and notify
    val updatedShards = MoveHelpers.readShards(nodes).mergedShards
    progressCallback.prepareSuccessful(
        shards = shards,
        targetNode = target,
        shardsDiff = MoveHelpers.calculateShardsDiff(initialShards, updatedShards)
    )
}
