package kolbasa.cluster.admin

import javax.sql.DataSource

/**
 * What to check on the cluster nodes before creating a [ClusterAdmin]
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
}
