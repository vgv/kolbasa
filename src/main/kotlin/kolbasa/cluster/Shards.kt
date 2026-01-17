package kolbasa.cluster

import kolbasa.schema.Const

internal data class Shards(val shards: List<Int>) {

    val asWhereClause = "${Const.SHARD_COLUMN_NAME} in (${shards.joinToString(separator = ",")})"

    companion object {
        val ALL_SHARDS = Shards((Shard.MIN_SHARD..Shard.MAX_SHARD).toList())
    }
}
