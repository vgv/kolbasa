package kolbasa.cluster

import kolbasa.consumer.datasource.Consumer
import kolbasa.producer.datasource.Producer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import javax.sql.DataSource

internal data class ClusterState(
    val nodes: Map<String, DataSource>,
    val shards: Map<Int, Shard>
) {

    private val producers = ConcurrentHashMap<ClusterProducer<*, *>, ConcurrentMap<String, Producer<*, *>>>()
    private val consumers = ConcurrentHashMap<ClusterConsumer<*, *>, ConcurrentMap<String, Consumer<*, *>>>()
    private val aliveConsumers = ConcurrentHashMap<ClusterConsumer<*, *>, ConcurrentMap<String, Consumer<*, *>>>()

    private val aliveConsumerNodes: List<String> = shards
        .values
        .mapNotNull { it.consumerNode }
        .distinct()

    private val aliveConsumerShards: Map<String, Shards> = aliveConsumerNodes.associateWith { node ->
        Shards(shards.filter { it.value.consumerNode == node }.keys.toList())
    }


    fun <Data, Meta : Any> getProducer(
        clusterProducer: ClusterProducer<Data, Meta>,
        shard: Int,
        compute: (DataSource) -> Producer<Data, Meta>
    ): Producer<Data, Meta> {
        val node = producerNode(shard)

        return producers
            .computeIfAbsent(clusterProducer) { _ ->
                ConcurrentHashMap()
            }
            .computeIfAbsent(node) { _ ->
                compute(dataSource(node))
            }
            as Producer<Data, Meta>
    }

    fun <Data, Meta : Any> getActiveConsumer(
        clusterConsumer: ClusterConsumer<Data, Meta>,
        compute: (DataSource, Shards) -> Consumer<Data, Meta>
    ): Consumer<Data, Meta>? {
        val randomNode = aliveConsumerNodes.random()

        return aliveConsumers
            .computeIfAbsent(clusterConsumer) { _ ->
                ConcurrentHashMap()
            }
            .computeIfAbsent(randomNode) { _ ->
                compute(dataSource(randomNode), aliveConsumerShards[randomNode]!!)
            }
            as Consumer<Data, Meta>
    }

    fun <Data, Meta : Any> getConsumer(
        clusterConsumer: ClusterConsumer<Data, Meta>,
        shard: Int,
        compute: (DataSource) -> Consumer<Data, Meta>
    ): Consumer<Data, Meta> {
        val node = consumerNode(shard)

        return consumers
            .computeIfAbsent(clusterConsumer) { _ ->
                ConcurrentHashMap()
            }
            .computeIfAbsent(node) { _ ->
                compute(dataSource(node))
            }
            as Consumer<Data, Meta>
    }

    private fun producerNode(shard: Int): String {
        return shards[shard]?.producerNode ?: throw IllegalArgumentException("Shard $shard is not found")
    }

    private fun consumerNode(shard: Int): String {
        return shards[shard]?.consumerNode ?: throw IllegalArgumentException("Shard $shard is not found")
    }

    private fun dataSource(node: String): DataSource {
        return nodes[node] ?: throw IllegalArgumentException("Node $node is not found")
    }

    companion object {
        val NOT_INITIALIZED = ClusterState(emptyMap(), emptyMap())
    }
}
