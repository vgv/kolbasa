package kolbasa.cluster

internal data class Shards(val shards: List<Int>) {

    // Text representation of the list of shards for use in SQL queries
    val asText = shards.joinToString(separator = ",")

    companion object {
        val ALL_SHARDS = Shards((Shard.MIN_SHARD..Shard.MAX_SHARD).toList())
    }
}
