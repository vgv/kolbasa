package kolbasa.cluster

import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import java.util.SortedMap
import javax.sql.DataSource

internal object ClusterHelper {

    fun readNodes(dataSources: List<DataSource>): SortedMap<Node, DataSource> {
        val allNodes = dataSources.mapIndexed { index, dataSource ->
            IdSchema.createAndInitIdTable(dataSource, identifierBucket = Node.MIN_BUCKET + index)
            val node = requireNotNull(IdSchema.readNodeInfo(dataSource)) {
                "Node info is not found, dataSource: $dataSource"
            }
            node to dataSource
        }

        // check uniqueness of nodeId
        checkNonUniqueNodeIds(allNodes.map { it.first.id })

        return allNodes.associateTo(sortedMapOf()) { it }
    }

    private fun checkNonUniqueNodeIds(nodeIds: List<NodeId>) {
        // Check nodeId uniqueness, maybe later I will add some kind of auto-fixing, but not now
        val nonUniqueNodeIds = nodeIds
            .groupBy { it }
            .filterValues { it.size > 1 }
            .keys

        check(nonUniqueNodeIds.isEmpty()) {
            "NodeId isn't unique, duplicated nodeIds: $nonUniqueNodeIds"
        }
    }

}
