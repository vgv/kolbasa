package kolbasa.cluster.butcher

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

internal interface ProgressCallback {

    fun prepareSuccessful(shards: List<ShardId>, targetNode: NodeId, shardsDiff: List<ShardDiff>)

    fun moveStart(tableName: String, from: DataSource, to: DataSource)
    fun moveEnd(tableName: String, from: DataSource, to: DataSource, migratedRows: Int)
    fun moveNextBatch(tableName: String, from: DataSource, to: DataSource, migratedRows: Int)

    fun finalizeSuccessful(shardsDiff: List<ShardDiff>)

}
