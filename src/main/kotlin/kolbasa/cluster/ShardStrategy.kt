package kolbasa.cluster

sealed class ShardStrategy {

    abstract fun getShard(): Int

    object Random : ShardStrategy() {
        override fun getShard(): Int = Shard.randomShard()
    }

    object ThreadLocal : ShardStrategy() {
        private val storage = object : java.lang.ThreadLocal<Int>() {
            override fun initialValue(): Int = Shard.randomShard()
        }

        override fun getShard(): Int = storage.get()
    }

    data class Fixed(val fixedShardValue: Int) : ShardStrategy() {
        override fun getShard(): Int = fixedShardValue
    }

}
