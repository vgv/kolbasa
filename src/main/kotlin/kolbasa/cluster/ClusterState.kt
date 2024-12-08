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

    // DataSources available to store data if primary shard producer is not available
    private val dataSourcesAvailableForProducers: List<DataSource> = shards
        .values
        .map { shard -> shard.producerNode }
        .distinct()
        .mapNotNull { producerNode -> nodes[producerNode] }

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
        generateProducer: (DataSource) -> Producer<Data, Meta>
    ): Producer<Data, Meta> {
        val nodeToProducers = producers.computeIfAbsent(clusterProducer) { _ ->
            ConcurrentHashMap()
        }

        val node = producerNode(shard)
        val producer = nodeToProducers.computeIfAbsent(node) { _ ->
            val dataSource = nodes.getOrElse(node) {
                // Alive node is not found, let's use any available node to store data because we have no other choice
                // It's better to store data on random node than to lose it
                // Eventually we will find these messages and migrate them to the correct node
                dataSourcesAvailableForProducers.random()
            }

            generateProducer(dataSource)
        }

        return producer as Producer<Data, Meta>
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
