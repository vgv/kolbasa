package kolbasa.cluster.butcher

import kolbasa.schema.NodeId
import javax.sql.DataSource

internal object ConsoleProgressCallback : ProgressCallback {

    override fun prepareSuccessful(shards: List<Int>, targetNode: NodeId, shardsDiff: List<ShardDiff>) {
        println("Prepare")
        println("Shards: $shards")
        println("Target node: ${targetNode.id}")
        println("Shards prepared to move (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            val originalShard =
                "[producerNode=${diff.originalShard.producerNode.id}, consumerNode=${diff.originalShard.consumerNode?.id}, nextConsumerNode=${diff.originalShard.nextConsumerNode?.id}]"
            val updatedShard =
                "[producerNode=${diff.updatedShard.producerNode.id}, consumerNode=${diff.updatedShard.consumerNode?.id}, nextConsumerNode=${diff.updatedShard.nextConsumerNode?.id}]"
            println("\tShard #${diff.originalShard.shard} $originalShard=>$updatedShard")
        }
    }

    override fun moveStart(tableName: String, from: DataSource, to: DataSource) {
        println("Move table '$tableName' from $from to $to")
    }

    override fun moveEnd(
        tableName: String,
        from: DataSource,
        to: DataSource,
        migratedRows: Int
    ) {
        println("Move table '$tableName' from $from to $to finished, migrated rows: $migratedRows")
    }

    override fun moveNextBatch(
        tableName: String,
        from: DataSource,
        to: DataSource,
        migratedRows: Int
    ) {
        println("Move table '$tableName' from $from to $to, migrated rows: $migratedRows")
    }

    override fun finalizeSuccessful(shardsDiff: List<ShardDiff>) {
        println("Finalize")
        println("Shards moved to stable state (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            val originalShard =
                "[producerNode=${diff.originalShard.producerNode.id}, consumerNode=${diff.originalShard.consumerNode?.id}, nextConsumerNode=${diff.originalShard.nextConsumerNode?.id}]"
            val updatedShard =
                "[producerNode=${diff.updatedShard.producerNode.id}, consumerNode=${diff.updatedShard.consumerNode?.id}, nextConsumerNode=${diff.updatedShard.nextConsumerNode?.id}]"
            println("\tShard #${diff.originalShard.shard} $originalShard=>$updatedShard")
        }
    }
}
