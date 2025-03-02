package kolbasa.cluster

import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import java.util.SortedMap
import javax.sql.DataSource

internal object ClusterHelper {

    fun readNodes(dataSources: List<DataSource>): SortedMap<Node, DataSource> {
        return dataSources
            .associateBy { dataSource ->
                IdSchema.createAndInitIdTable(dataSource)
                requireNotNull(IdSchema.readNodeInfo(dataSource)) {
                    "Node info is not found, dataSource: $dataSource"
                }
            }
            .toSortedMap()
    }

    fun checkNonUniqueServerIds(nodes: SortedMap<Node, DataSource>) {
        // Check serverId uniqueness, maybe later I will add some kind of auto-fixing, but not now
        val serverIds = nodes.map { it.key.serverId }

        val nonUniqueServerIds = serverIds
            .groupBy { it }
            .filterValues { it.size > 1 }
            .keys

        check(nonUniqueServerIds.isEmpty()) {
            "ServerId isn't unique, duplicated serverIds: $nonUniqueServerIds"
        }
    }

}
