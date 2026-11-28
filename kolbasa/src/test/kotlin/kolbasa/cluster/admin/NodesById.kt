package kolbasa.cluster.admin

import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Gives every node of [dataSources] an id the way a [Cluster][kolbasa.cluster.Cluster] does, and returns the nodes by
 * those ids, the way [ClusterAdmin] takes them
 *
 * The ids are random, so a test takes them from the result rather than naming them.
 */
internal fun nodesById(dataSources: List<DataSource>): Map<NodeId, DataSource> {
    val nodes = ClusterHelpers.readOrCreateNodes(dataSources)
    return nodes.entries.associate { (node, dataSource) -> node.id to dataSource }
}
