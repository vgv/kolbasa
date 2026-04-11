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
            println("\t${diff.originalShard} => ${diff.updatedShard}")
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
