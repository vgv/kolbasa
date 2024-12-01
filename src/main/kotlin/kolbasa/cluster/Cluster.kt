package kolbasa.cluster

import java.util.*
import javax.sql.DataSource

class Cluster(private val dataSources: () -> List<DataSource>) {

    constructor(dataSources: List<DataSource>) : this({ dataSources })

    @Volatile
    private lateinit var state: ClusterState

    @Synchronized
    fun update() {
        val dataSources = dataSources()
        if (dataSources.isEmpty()) {
            throw IllegalArgumentException("Data sources list is empty")
        }

        val nodes = initNodes(dataSources)
        val shards = initShards(nodes)

        val newState = ClusterState(nodes, shards)
        if (!this::state.isInitialized || newState != this.state) {
            this.state = newState
        }
    }

    private fun initNodes(dataSources: List<DataSource>): SortedMap<String, DataSource> {
        return dataSources
            .associateBy { dataSource ->
                IdSchema.createAndInitIdTable(dataSource)
                requireNotNull(IdSchema.readNodeId(dataSource)) {
                    "Node info is not found, dataSource: $dataSource"
                }
            }.toSortedMap()
    }

    private fun initShards(nodes: SortedMap<String, DataSource>): Map<Int, Shard> {
        val existingShardTable = findShardTable(nodes)
        if (existingShardTable != null) {
            return existingShardTable
        }

        // No filled shard table at all, fill it with random nodes
        // First, find a node with the smallest id
        val firstDataSource = nodes.values.first()
        ShardSchema.fillShardTable(firstDataSource, nodes.keys.toList())
        return ShardSchema.readShards(firstDataSource)
    }

    private fun findShardTable(nodes: Map<String, DataSource>): Map<Int, Shard>? {
        return nodes
            .mapValues { (_, dataSource) ->
                ShardSchema.createShardTable(dataSource)
                ShardSchema.readShards(dataSource)
            }
            .filter { (_, shards) ->
                shards.isNotEmpty()
            }
            .toSortedMap()
            .values
            .firstOrNull()
    }

    internal fun getState() = state

}
