package kolbasa.cluster.migrate

import kolbasa.cluster.Shard
import kolbasa.schema.Node
import kolbasa.schema.Table
import javax.sql.DataSource

internal interface MigrateEvents {

    fun prepareMigrateToTheSameShardError(shard: Shard, targetNode: String)
    fun prepareMigrateToNonExistingNodeError(knownNodes: List<Node>, targetNode: String)
    fun prepareSuccessful(shards: List<Int>, targetNode: String, shardsDiff: List<ShardDiff>)

    fun migrateInconsistentSchemaError(tableName: String, sourceSchema: Table, targetSchema: Table)
    fun migrateStart(tableName: String, from: DataSource, to: DataSource)
    fun migrateEnd(tableName: String, from: DataSource, to: DataSource, migratedRows: Int)
    fun migrateNextBatch(tableName: String, from: DataSource, to: DataSource, migratedRows: Int)

}
