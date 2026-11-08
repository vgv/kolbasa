package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.producer.ProducerOptions
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue
import java.util.concurrent.CompletableFuture

/**
 * A [Producer] that sends messages to a cluster of nodes.
 *
 * Every message gets a shard number, and the shard decides which node stores it. The number comes from the first of
 * these that is set: the shard of the `send()` call ([SendOptions.shard][kolbasa.producer.SendOptions.shard]), the
 * shard of this producer ([ProducerOptions.shard][kolbasa.producer.ProducerOptions.shard]), or the shard that
 * `shardStrategy` of this producer picks. Any number is accepted: it is folded into the `0..1023` range for you.
 *
 * One `send()` call goes to one node, so a batch of messages that must land together must share a shard. If the
 * node that owns the shard is not part of the cluster at that moment, the messages are written to another node
 * instead of being lost. No consumer receives them there, because a consumer reads only the shards its node owns.
 * They stay there: kolbasa has no procedure yet to move them to the node that owns their shard.
 *
 * This class is a thin wrapper around a [Cluster]: it holds no connections of its own and creates a plain
 * [DatabaseProducer][kolbasa.producer.datasource.DatabaseProducer] per node behind the scenes. Creating one is
 * cheap, but `options` and `shardStrategy` are fixed at that moment, so keep one instance per set of defaults you
 * need. The [Cluster] must be initialized before the first `send()`: by a [ClusterTopologyUpdater], or by your own
 * call to [Cluster.updateTopology].
 *
 * This class implements the [Producer] interface, so it's easy to use instead of [DatabaseProducer] if you need to
 * migrate from a single-node setup to a Kolbasa cluster.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val producer: Producer = ClusterProducer(cluster)
 *
 * producer.send(orders, "message")
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Producer producer = new ClusterProducer(cluster);
 *
 * producer.send(orders, "message");
 * ```
 *
 * @param cluster the cluster to send to
 * @param options the defaults of every `send()` of this producer
 * @param shardStrategy picks the shard of a message when neither the `send()` call nor [options] sets one, see
 * [ShardStrategy]. The default, [ShardStrategy.ThreadLocalWithInterval], keeps the messages of one thread on one
 * shard for five minutes at a time.
 * @see Cluster
 * @see kolbasa.producer.datasource.DatabaseProducer
 */
class ClusterProducer @JvmOverloads constructor(
    private val cluster: Cluster,
    override val options: ProducerOptions = ProducerOptions.DEFAULT,
    private val shardStrategy: ShardStrategy = ShardStrategy.ThreadLocalWithInterval()
) : Producer {

    override fun <Data> send(queue: Queue<Data>, request: SendRequest<Data>): SendResult<Data> {
        request.effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(
            sendOptions = request.options,
            producerOptions = options,
            shardStrategy = shardStrategy
        )

        val topology = cluster.getTopology()
        val producer = topology.getProducer(this, request.effectiveShard) { nodeId, dataSource ->
            val p = ConnectionAwareDatabaseProducer(nodeId, options)
            DatabaseProducer(dataSource, p)
        }

        return producer.send(queue, request)
    }

    override fun <Data> sendAsync(queue: Queue<Data>, request: SendRequest<Data>): CompletableFuture<SendResult<Data>> {
        // TODO: make it smarter
        val executor = ProducerSchemaHelpers.calculateAsyncExecutor(
            callExecutor = request.options.asyncExecutor,
            producerExecutor = options.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )
        val tracing = options.tracing ?: Kolbasa.tracing

        return CompletableFuture.supplyAsync(tracing.wrapAsync { send(queue, request) }, executor)
    }
}
