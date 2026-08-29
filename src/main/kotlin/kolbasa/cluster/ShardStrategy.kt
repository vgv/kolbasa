package kolbasa.cluster

import java.time.Duration

/**
 * Chooses the shard of a message when nothing else does.
 *
 * A shard decides which node of a cluster stores a message. A message can get its shard from three places, and the
 * first one that is set wins: the `send()` call ([SendOptions.shard][kolbasa.producer.SendOptions.shard]), the
 * producer ([ProducerOptions.shard][kolbasa.producer.ProducerOptions.shard]), and – when neither is set – the
 * strategy in [Kolbasa.shardStrategy][kolbasa.Kolbasa.shardStrategy]. So a strategy is the default for the messages
 * you did not place by hand.
 *
 * The strategy is read on every send, and only by a cluster producer
 * ([ClusterProducer]); a single-node producer stores every message with shard `0`.
 *
 * Which one to pick:
 * - [Random] spreads messages over the whole cluster as evenly as possible.
 * - [Fixed] sends everything to one shard, and therefore to one node.
 * - [ThreadLocal] keeps the messages of one thread together for the life of that thread.
 * - [ThreadLocalWithInterval] keeps them together for a while, then moves on. This is the default.
 *
 * Messages that share a shard are stored on the same node, which is worth knowing when you want related messages to
 * arrive together. Messages with different shards may be split over many nodes.
 *
 * ## Usage Example
 *
 * ```kotlin
 * // Spread every message over the cluster
 * Kolbasa.shardStrategy = ShardStrategy.Random
 *
 * // Or keep the messages of one thread together for five minutes at a time
 * Kolbasa.shardStrategy = ShardStrategy.ThreadLocalWithInterval(Duration.ofMinutes(5))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setShardStrategy(ShardStrategy.Random.INSTANCE);
 *
 * Kolbasa.setShardStrategy(new ShardStrategy.ThreadLocalWithInterval(Duration.ofMinutes(5)));
 * ```
 *
 * You can also write your own strategy, but there is no reason to: the four below cover the useful cases, and a
 * strategy that needs to look at the message itself belongs in
 * [SendOptions.shard][kolbasa.producer.SendOptions.shard] instead.
 *
 * Why a shard matters at all, which node a given shard lives on, and what co-location buys you are described in
 * [Cluster architecture](https://github.com/vgv/kolbasa/blob/main/docs/Cluster%20architecture.md#how-a-messages-shard-is-chosen).
 */
sealed class ShardStrategy {

    /**
     * Returns the shard for the next message.
     *
     * Called once per `send()` call, so it must be fast and safe to call from many threads at once.
     *
     * Any [Int] is allowed. Kolbasa folds the value into the `0..1023` range itself
     * (from [Shard.MIN_SHARD] to [Shard.MAX_SHARD]), so a strategy may return, for example, a hash code without doing
     * anything about its size or sign.
     */
    abstract fun getShard(): Int

    /**
     * A new random shard for every message.
     *
     * This spreads the load over the cluster as evenly as it can. Messages sent one after another usually land on
     * different nodes, so nothing is co-located.
     */
    object Random : ShardStrategy() {
        override fun getShard(): Int = Shard.randomShard()
    }

    /**
     * The same shard for every message.
     *
     * Everything this strategy sends goes to one node. It is useful when one process must keep its messages
     * together, and in tests, where a known shard makes the result predictable.
     *
     * @property fixedShardValue the shard to use. Any [Int] is allowed, it is folded into the `0..1023` range.
     */
    data class Fixed(val fixedShardValue: Int) : ShardStrategy() {
        override fun getShard(): Int = fixedShardValue
    }

    /**
     * One random shard per thread, chosen when that thread sends its first message and never changed.
     *
     * Every message of one thread lands on one node, which is good for co-location and for the database: fewer
     * nodes touched per thread means fewer connections and larger batches per node.
     *
     * Be careful with long-lived threads. A pool of ten threads pins your whole application to at most ten shards
     * for as long as it runs, and those shards may well belong to the same node. [ThreadLocalWithInterval] exists
     * to avoid exactly that.
     */
    object ThreadLocal : ShardStrategy() {
        private val storage = object : java.lang.ThreadLocal<Int>() {
            override fun initialValue(): Int = Random.getShard()
        }

        override fun getShard(): Int = storage.get()
    }

    /**
     * One random shard per thread, replaced by a new one when the interval has passed.
     *
     * This is the default strategy of Kolbasa, and it is a compromise between the two above. Inside the interval it
     * behaves like [ThreadLocal] – all messages of a thread go to one node. After the interval the thread picks a
     * new random shard, so over hours the load is spread like [Random].
     *
     * The interval is checked on send, not by a timer: a thread that stops sending keeps its shard until it sends
     * again.
     *
     * @property interval how long one thread keeps the same shard. 15 minutes by default.
     */
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
