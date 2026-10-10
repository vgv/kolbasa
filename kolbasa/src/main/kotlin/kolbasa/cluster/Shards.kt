package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.Const

internal data class Shards(val shards: List<ShardId>) {

    val asWhereClause = "${Const.SHARD_COLUMN_NAME} in (${shards.joinToString(separator = ",") { it.id.toString() }})"

    companion object {
        val ALL_SHARDS = Shards(ShardId.SHARDS_RANGE.map(ShardId::of))
    }
}
