package kolbasa.cluster

import java.time.Duration

sealed class ShardStrategy {

    abstract fun getShard(): Int

    object Random : ShardStrategy() {
        override fun getShard(): Int = Shard.randomShard()
    }

    data class Fixed(val fixedShardValue: Int) : ShardStrategy() {
        override fun getShard(): Int = fixedShardValue
    }

    object ThreadLocal : ShardStrategy() {
        private val storage = object : java.lang.ThreadLocal<Int>() {
            override fun initialValue(): Int = Random.getShard()
        }

        override fun getShard(): Int = storage.get()
    }

    data class ThreadLocalWithInterval(val interval: Duration = Duration.ofMinutes(15)) : ShardStrategy() {

        private val intervalMillis = interval.toMillis()

        private val storage = object : java.lang.ThreadLocal<Pair<Long, Int>>() {
            override fun initialValue(): Pair<Long, Int> = generateNewPair()
        }

        override fun getShard(): Int {
            val (created, shard) = storage.get()

            if ((System.currentTimeMillis() - created) < intervalMillis) {
                return shard
            } else {
                val newPair = generateNewPair()
                storage.set(newPair)
                return newPair.second
            }
        }

        private fun generateNewPair() = Pair(System.currentTimeMillis(), Random.getShard())
    }


}
