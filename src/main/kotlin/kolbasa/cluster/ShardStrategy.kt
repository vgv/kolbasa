package kolbasa.cluster

sealed class ShardStrategy {

    abstract fun getShard(): Int

    object Random : ShardStrategy() {
        override fun getShard(): Int = Shard.randomShard()
    }

    data class Fixed(val shard: Int) : ShardStrategy() {
        override fun getShard(): Int = shard
    }

    object ThreadLocal : ShardStrategy() {
        private val storage = object : java.lang.ThreadLocal<Int>() {
            override fun initialValue(): Int = Shard.randomShard()
        }

        override fun getShard(): Int = storage.get()
    }

}
