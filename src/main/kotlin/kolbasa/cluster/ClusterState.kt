package kolbasa.cluster

import kolbasa.producer.datasource.Producer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import javax.sql.DataSource

internal data class ClusterState(
    val nodes: Map<String, DataSource>,
    val shards: Map<Int, Shard>
) {

    private val producers = ConcurrentHashMap<ClusterProducer<*, *>, ConcurrentMap<String, Producer<*, *>>>()

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

    private fun producerNode(shard: Int): String {
        return shards[shard]?.producerNode ?: throw IllegalArgumentException("Shard $shard is not found")
    }

    private fun dataSource(node: String): DataSource {
        return nodes[node] ?: throw IllegalArgumentException("Node $node is not found")
    }
}
