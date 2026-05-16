package kolbasa.cluster.butcher

import kolbasa.schema.NodeId
import javax.sql.DataSource

internal object ConsoleProgressCallback : ProgressCallback {

    override fun prepareSuccessful(shards: List<Int>, targetNode: NodeId, shardsDiff: List<ShardDiff>) {
        println("Prepare")
        println("Shards: $shards")
        println("Target node: ${targetNode.id}")
        println("Shards to move (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            // Shard(shard=6, producerNode=NodeId(id=db4), consumerNode=NodeId(id=db4), nextConsumerNode=null) => Shard(shard=6, producerNode=NodeId(id=db5), consumerNode=null, nextConsumerNode=NodeId(id=db5))
            val originalShard =
                "Shard #${diff.originalShard.shard}(producerNode=${diff.originalShard.producerNode.id}, consumerNode=${diff.originalShard.consumerNode?.id}, nextConsumerNode=${diff.originalShard.nextConsumerNode?.id})"
            val updatedShard =
                "Shard #${diff.updatedShard.shard}(producerNode=${diff.updatedShard.producerNode.id}, consumerNode=${diff.updatedShard.consumerNode?.id}, nextConsumerNode=${diff.updatedShard.nextConsumerNode?.id})"
            println("\t$originalShard=>$updatedShard")
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
}
