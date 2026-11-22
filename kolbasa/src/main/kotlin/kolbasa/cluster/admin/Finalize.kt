package kolbasa.cluster.admin

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import javax.sql.DataSource

internal fun finalize(dataSources: List<DataSource>, progressCallback: ProgressCallback) {
    val nodes = ClusterHelper.readNodes(dataSources)
    val shardMap = MoveHelpers.readOrCreateShards(nodes)
    val initialShards = shardMap.mergedShards

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
    MoveHelpers.writeShards(nodes, shardMap, newRows)

    // Calculate difference between initial and updated shards and notify
    val updatedShards = MoveHelpers.readShards(nodes).mergedShards
    progressCallback.finalizeSuccessful(
        shardsDiff = MoveHelpers.calculateShardsDiff(initialShards, updatedShards)
    )
}
