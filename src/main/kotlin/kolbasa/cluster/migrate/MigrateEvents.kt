package kolbasa.cluster.migrate

import javax.sql.DataSource

internal interface MigrateEvents {

    fun prepareSuccessful(shards: List<Int>, targetNode: String, shardsDiff: List<ShardDiff>)

    fun migrateStart(tableName: String, from: DataSource, to: DataSource)
    fun migrateEnd(tableName: String, from: DataSource, to: DataSource, migratedRows: Int)
    fun migrateNextBatch(tableName: String, from: DataSource, to: DataSource, migratedRows: Int)

}
