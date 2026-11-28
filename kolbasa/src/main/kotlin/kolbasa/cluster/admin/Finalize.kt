package kolbasa.cluster.admin

import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.schema.NodeId
import javax.sql.DataSource

internal fun finalize(dataSources: Map<NodeId, DataSource>, progressCallback: ProgressCallback) {
    val nodes = MoveHelpers.readNodes(dataSources)
    val initialShards = MoveHelpers.readCompleteShards(nodes)

    // Okay, everything looks good, let's switch the migrating shards to a stable state
    val revision = ShardSchema.nextRevision(initialShards.values)
    val newRows = mutableListOf<Shard>()
    for (shard in initialShards.values) {
        if (shard.consumerNode == null) {
            newRows += Shard(
                shard = shard.shard,
                producerNode = shard.producerNode,
                consumerNode = shard.nextConsumerNode,
                nextConsumerNode = null,
                revision = revision
            )
        }
    }
    MoveHelpers.writeShards(nodes, newRows)

    // Calculate difference between initial and updated shards and notify
    val updatedShards = MoveHelpers.readShards(nodes).mergedShards
    progressCallback.finalizeSuccessful(
        shardsDiff = MoveHelpers.calculateShardsDiff(initialShards, updatedShards)
    )
}
