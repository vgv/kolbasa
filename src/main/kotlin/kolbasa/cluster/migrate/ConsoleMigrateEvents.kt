package kolbasa.cluster.migrate

import kolbasa.cluster.Shard
import kolbasa.schema.Node
import kolbasa.schema.Table
import javax.sql.DataSource

internal class ConsoleMigrateEvents : MigrateEvents {

    override fun prepareMigrateToTheSameShardError(shard: Shard, targetNode: String) {
        println("Error: $shard is already on node '$targetNode', you can't move shard to the same node")
    }

    override fun prepareMigrateToNonExistingNodeError(knownNodes: List<Node>, targetNode: String) {
        println("Error: Node '$targetNode' not found, known nodes: $knownNodes")
    }

    override fun prepareSuccessful(shards: List<Int>, targetNode: String, shardsDiff: List<ShardDiff>) {
        println("Prepare")
        println("Shards: $shards")
        println("Target node: $targetNode")
        println("Shards to move (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            println("\t${diff.originalShard} => ${diff.updatedShard}")
        }
    }

    override fun migrateInconsistentSchemaError(
        tableName: String,
        sourceSchema: Table,
        targetSchema: Table
    ) {
        println("Error: Inconsistent schema for table '$tableName' ")
        println("Source schema: $sourceSchema")
        println("Target schema: $targetSchema")
    }

    override fun migrateStart(tableName: String, from: DataSource, to: DataSource) {
        println("Migrate table '$tableName' from $from to $to")
    }

    override fun migrateEnd(
        tableName: String,
        from: DataSource,
        to: DataSource,
        migratedRows: Int
    ) {
        println("Migrate table '$tableName' from $from to $to finished, migrated rows: $migratedRows")
    }

    override fun migrateNextBatch(
        tableName: String,
        from: DataSource,
        to: DataSource,
        migratedRows: Int
    ) {
        println("Migrate table '$tableName' from $from to $to, migrated rows: $migratedRows")
    }
}
