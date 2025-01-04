package kolbasa.cluster

import javax.sql.DataSource

class Cluster(private val dataSources: () -> List<DataSource>) {

    constructor(dataSources: List<DataSource>) : this({ dataSources })

    @Volatile
    private var state: ClusterState = ClusterState.NOT_INITIALIZED

    @Synchronized
    fun updateState() {
        val dataSources = dataSources()
        if (dataSources.isEmpty()) {
            throw IllegalArgumentException("Data sources list is empty")
        }

        val nodes = initNodes(dataSources)
        val shards = initShards(nodes)

        val newState = ClusterState(nodes, shards)
        if (newState != state) {
            state = newState
        }
    }

    private fun initNodes(dataSources: List<DataSource>): Map<String, DataSource> {
        return dataSources
            .associateBy { dataSource ->
                IdSchema.createAndInitIdTable(dataSource)
                requireNotNull(IdSchema.readNodeId(dataSource)) {
                    "Node info is not found, dataSource: $dataSource"
                }
            }
    }

    private fun initShards(nodes: Map<String, DataSource>): Map<Int, Shard> {
        val sortedNodes = nodes.toSortedMap()

        // First, try to find a node with a 100% initialized shard table, if found, return it
        for ((_, dataSource) in sortedNodes) {
            val shards = try {
                ShardSchema.readShards(dataSource)
            } catch (e: Exception) {
                // Shard table doesn't exist
                emptyMap()
            }

            // Shard table is 100% initialized, return it
            if (shards.size == Shard.SHARD_COUNT) {
                return shards
            }
        }

        // No fully initialized shard table found, let's create a
        // fully initialized shard table on the node with the smallest id
        val (_, dataSource) = sortedNodes.entries.first()
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
        return ShardSchema.readShards(dataSource)
    }

    internal fun getState(): ClusterState {
        check(state !== ClusterState.NOT_INITIALIZED) {
            "Cluster state isn't initialized, maybe you forgot to call updateState() method?"
        }

        return state
    }

}
