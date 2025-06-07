package kolbasa.cluster

import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import java.util.SortedMap
import javax.sql.DataSource

internal object ClusterHelper {

    fun readNodes(dataSources: List<DataSource>): SortedMap<Node, DataSource> {
        val allNodes = dataSources.map { dataSource ->
            IdSchema.createAndInitIdTable(dataSource)
            val node = requireNotNull(IdSchema.readNodeInfo(dataSource)) {
                "Node info is not found, dataSource: $dataSource"
            }
            node to dataSource
        }

        // check uniqueness of serverId
        checkNonUniqueServerIds(allNodes.map { it.first.id })

        return allNodes.associateTo(sortedMapOf()) { it }
    }

    private fun checkNonUniqueServerIds(nodeIds: List<NodeId>) {
        // Check serverId uniqueness, maybe later I will add some kind of auto-fixing, but not now
        val nonUniqueServerIds = nodeIds
            .groupBy { it }
            .filterValues { it.size > 1 }
            .keys

        check(nonUniqueServerIds.isEmpty()) {
            "ServerId isn't unique, duplicated serverIds: $nonUniqueServerIds"
        }
    }

}
