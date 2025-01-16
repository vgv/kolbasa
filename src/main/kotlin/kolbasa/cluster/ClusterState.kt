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

    // -------------------------------------------------------------------------------------------------
    // DataSources available to store data if some shard producer is not available
    private val dataSourcesWithActiveProducers: List<DataSource> by lazy {
        // Collect all known producer nodes from shard table.
        // Shard table can contain references to nodes that are not in the cluster, but at this step
        // we don't care about it, because we will check the availability of nodes later.
        // Just collect all producer nodes names
        val allKnownProducerNodes = shards
            .values
            .map { shard -> shard.producerNode }
            .distinct()

        // Second step - filter all possible producer nodes names and leave only those that are in the cluster
        val existingProducerNodes = allKnownProducerNodes
            .mapNotNull { producerNode -> nodes[producerNode] }

        existingProducerNodes
    }

    // List of nodes that have active consumers or, in other words, nodes from which we can read data
    private val activeConsumerNodes: List<String> by lazy {
        // Collect all known consumer nodes from shard table.
        // Shard table can contain references to nodes that are not in the cluster, but at this step
        // we don't care about it, because we will check the availability of nodes later.
        // Just collect all consumer nodes names
        val allKnownConsumerNodes = shards
            .values
            .mapNotNull { shard -> shard.consumerNode }
            .distinct()

        // Second step - filter all possible consumer nodes names and leave only those that are in the cluster
        val existingConsumerNodes = allKnownConsumerNodes
            .filter { consumerNode -> nodes.containsKey(consumerNode) }

        existingConsumerNodes
    }

    // Shards that are assigned to each active consumer node right now
    // For example, we had 100 shards on one node, but one shard started migrating to another node. In this case, we
    // will have 99 shards on this particular node
    // node -> shard[]
    private val activeConsumerNodesToShards: Map<String, Shards> by lazy {
        activeConsumerNodes.associateWith { node ->
            // Collect all shards that are assigned to this node right now
            val thisNodeShards = shards
                .filter { it.value.consumerNode == node }
                .keys
                .toList()

            Shards(thisNodeShards)
        }
    }

    // Map of active shards to active consumer nodes
    // shard -> node
    private val activeConsumerShardsToNodes: Map<Int, String> by lazy {
        val result = hashMapOf<Int, String>()
        activeConsumerNodesToShards.forEach { (node, shards) ->
            shards.shards.forEach { shard ->
                result[shard] = node
            }
        }

        result
    }

    // -------------------------------------------------------------------------------------------------

    private val producers: ConcurrentMap<ClusterProducer<*, *>, ConcurrentMap<String, Producer<*, *>>> by lazy {
        ConcurrentHashMap()
    }

    private val activeConsumers: ConcurrentMap<ClusterConsumer<*, *>, ConcurrentMap<String, Consumer<*, *>>> by lazy {
        ConcurrentHashMap()
    }

    private val allConsumers: ConcurrentMap<ClusterConsumer<*, *>, ConcurrentMap<String, Consumer<*, *>>> by lazy {
        ConcurrentHashMap()
    }

    // -------------------------------------------------------------------------------------------------


    fun <Data, Meta : Any> getProducer(
        clusterProducer: ClusterProducer<*, *>,
        shard: Int,
        generateProducer: (DataSource) -> Producer<Data, Meta>
    ): Producer<Data, Meta> {
        val nodeToProducers = producers.computeIfAbsent(clusterProducer) { _ ->
            ConcurrentHashMap()
        }

        val node = shards[shard]?.producerNode ?: throw IllegalArgumentException("Shard $shard is not found")

        val producer = nodeToProducers.computeIfAbsent(node) { _ ->
            val dataSource = nodes.getOrElse(node) {
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
            val shards =
                activeConsumerNodesToShards[randomNode] ?: throw IllegalStateException("Shards for node $randomNode not found")
            generateConsumer(dataSource, shards)
        }

        return consumer as Consumer<Data, Meta>
    }

    fun <T> mapShardsToNodes(list: List<T>, shardFunc: (T) -> Int): Map<String?, List<T>> {
        return list.groupBy { item ->
            val shard = shardFunc(item)
            activeConsumerShardsToNodes[shard]
        }
    }

    fun <Data, Meta : Any> getConsumer(
        clusterConsumer: ClusterConsumer<Data, Meta>,
        node: String,
        generateConsumer: (DataSource) -> Consumer<Data, Meta>
    ): Consumer<Data, Meta> {
        val nodesToConsumers = allConsumers.computeIfAbsent(clusterConsumer) { _ ->
            ConcurrentHashMap()
        }

        val consumer = nodesToConsumers.computeIfAbsent(node) { _ ->
            val dataSource = nodes[node] ?: throw IllegalStateException("Node $node not found")
            generateConsumer(dataSource)
        }

        return consumer as Consumer<Data, Meta>
    }

    fun <Data, Meta : Any> getConsumers(
        clusterConsumer: ClusterConsumer<Data, Meta>,
        generateConsumer: (DataSource) -> Consumer<Data, Meta>
    ): List<Consumer<Data, Meta>> {
        val nodesToConsumers = allConsumers.computeIfAbsent(clusterConsumer) { _ ->
            ConcurrentHashMap()
        }

        val consumers = nodes.map { (node, dataSource) ->
            nodesToConsumers.computeIfAbsent(node) { _ ->
                generateConsumer(dataSource)
            }
        }

        return consumers as List<Consumer<Data, Meta>>
    }

    companion object {
        val NOT_INITIALIZED = ClusterState(emptyMap(), emptyMap())
    }
}
