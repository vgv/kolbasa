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

    // Consumers ready to receive messages
    private val activeConsumers = ConcurrentHashMap<ClusterConsumer<*, *>, ConcurrentMap<String, Consumer<*, *>>>()
    private val consumers = ConcurrentHashMap<ClusterConsumer<*, *>, ConcurrentMap<String, Consumer<*, *>>>()


    // DataSources available to store data if primary shard producer is not available
    private val dataSourcesWithActiveProducers: List<DataSource> = shards
        .values
        .map { shard -> shard.producerNode }
        .distinct()
        .mapNotNull { producerNode -> nodes[producerNode] }

    private val activeConsumerNodes: List<String> = shards
        .values
        .mapNotNull { it.consumerNode }
        .distinct()
        .filter { consumerNode -> nodes.containsKey(consumerNode) }

    private val activeConsumerShards: Map<String, Shards> = activeConsumerNodes.associateWith { node ->
        val thisNodeShards = shards
            .filter { it.value.consumerNode == node }
            .keys
            .toList()
        Shards(thisNodeShards)
    }

    fun <Data, Meta : Any> getProducer(
        clusterProducer: ClusterProducer<Data, Meta>,
        shard: Int,
        generateProducer: (DataSource) -> Producer<Data, Meta>
    ): Producer<Data, Meta> {
        val nodeToProducers = producers.computeIfAbsent(clusterProducer) { _ ->
            ConcurrentHashMap()
        }

        val nodeId = producerNode(shard)
        val producer = nodeToProducers.computeIfAbsent(nodeId) { _ ->
            val dataSource = nodes.getOrElse(nodeId) {
                // Node with nodeId not found, let's use any available node to store data because we have no other choice
                // It's better to store data on random node than to lose it
                // Eventually we will find these messages and migrate them to the correct node
                // The algorithm of choosing a node is as follows:
                // 1) We try to avoid nodes without active producers, because such nodes are likely to be in the
                //    process of data migration, and we don't want to add more data to them
                // 2) However, if all nodes have no active producers, then this means, firstly, that this is a very strange
                //    cluster state ¯\_(ツ)_/¯, and secondly, that we have no choice and have to return literally any node
                dataSourcesWithActiveProducers.randomOrNull() ?: nodes.values.random()
            }

            generateProducer(dataSource)
        }

        return producer as Producer<Data, Meta>
    }

    fun <Data, Meta : Any> getActiveConsumer(
        clusterConsumer: ClusterConsumer<Data, Meta>,
        generateConsumer: (DataSource, Shards) -> Consumer<Data, Meta>
    ): Consumer<Data, Meta>? {
        val nodesToConsumers = activeConsumers.computeIfAbsent(clusterConsumer) { _ ->
            ConcurrentHashMap()
        }

        val randomNode = activeConsumerNodes.randomOrNull() ?: return null

        val consumer = nodesToConsumers.computeIfAbsent(randomNode) { _ ->
            val dataSource = nodes[randomNode] ?: throw IllegalStateException("Node $randomNode not found")
            val shards = activeConsumerShards[randomNode] ?: throw IllegalStateException("Shards for node $randomNode not found")
            generateConsumer(dataSource, shards)
        }

        return consumer as Consumer<Data, Meta>
    }

    fun <Data, Meta : Any> getConsumer(
        clusterConsumer: ClusterConsumer<Data, Meta>,
        shard: Int,
        generateConsumer: (DataSource) -> Consumer<Data, Meta>
    ): Consumer<Data, Meta> {
        val node = consumerNode(shard)

        return consumers
            .computeIfAbsent(clusterConsumer) { _ ->
                ConcurrentHashMap()
            }
            .computeIfAbsent(node) { _ ->
                generateConsumer(dataSource(node))
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
