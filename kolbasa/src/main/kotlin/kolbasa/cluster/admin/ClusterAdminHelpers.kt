package kolbasa.cluster.admin

import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Checks to run on the cluster nodes before creating a [ClusterAdmin]
 */
object ClusterAdminHelpers {

    /**
     * Checks that every node can be reached, and fails describing the ones that cannot
     *
     * [ClusterAdmin] assumes the cluster is reachable and reports a connection failure as it comes. Call this
     * first to get one clear error instead.
     *
     * @param nodes one [DataSource] per cluster node
     * @throws ClusterOperationException.ExecutionException if a node can't be reached
     */
    @JvmStatic
    fun checkNodesAvailable(nodes: List<DataSource>) {
        MoveHelpers.checkClusterNodes(nodes)
    }

    /**
     * Checks that every node has the id it was given, and gives an id to the nodes that have none yet
     *
     * A node without an id gets the one from [nodes]. A node with a different id fails the check, and so does
     * a `q__node` table without its row. If an existing node fails, new nodes are not given ids either: a
     * wrong id means the node list is wrong, and a wrong list must not set up anything.
     *
     * A new node gets the lowest bucket no other node uses.
     *
     * Call it after [checkNodesAvailable], before anything else touches the nodes. [ClusterAdmin] and
     * [Cluster][kolbasa.cluster.Cluster] give a node with no id a random one.
     *
     * @param nodes every cluster node by the id it should have
     * @return the nodes this call gave an id to
     * @throws ClusterOperationException.NodeIdMismatchException if a node has a different id, or a `q__node`
     * table without its row
     */
    @JvmStatic
    fun checkNodeIds(nodes: Map<NodeId, DataSource>): List<NodeId> {
        // Nodes without q__node are left out. A node with an empty q__node maps to null
        val existingNodes = nodes
            .filterValues { dataSource -> IdSchema.nodeTableExists(dataSource) }
            .mapValues { (_, dataSource) -> IdSchema.findNodeInfo(dataSource) }

        throwIfMismatched(existingNodes.mapValues { (_, node) -> node?.id })

        // Sorted, so the same node list always gets the same buckets
        val newNodes = nodes.filterKeys { nodeId -> nodeId !in existingNodes }.toSortedMap()
        val usedBuckets = existingNodes.values.mapNotNull { node -> node?.identifiersBucket }.toMutableSet()

        val setUp = mutableListOf<NodeId>()
        newNodes.forEach { (nodeId, dataSource) ->
            val bucket = (Node.MIN_BUCKET..Node.MAX_BUCKET).first { bucket -> bucket !in usedBuckets }
            usedBuckets += bucket

            if (IdSchema.createAndInitIdTable(dataSource, nodeId, bucket)) {
                setUp += nodeId
            }
        }

        // Read the new nodes again. Another process may have set one up at the same moment, or two ids may
        // point at the same database
        throwIfMismatched(newNodes.mapValues { (_, dataSource) -> IdSchema.findNodeInfo(dataSource)?.id })

        return setUp
    }

    private fun throwIfMismatched(actualIds: Map<NodeId, NodeId?>) {
        val mismatches = actualIds
            .mapNotNull { (expected, actual) -> if (actual != null && actual != expected) expected to actual else null }
            .toMap()
        val nodesWithoutId = actualIds.filterValues { actual -> actual == null }.keys.toList()

        if (mismatches.isNotEmpty() || nodesWithoutId.isNotEmpty()) {
            throw ClusterOperationException.NodeIdMismatchException(mismatches, nodesWithoutId)
        }
    }
}
