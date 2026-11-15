package kolbasa.cluster

import kolbasa.schema.NodeId
import kolbasa.schema.SchemaResult

/**
 * What the schema update of a cluster did on every node, see [Cluster.updateTopology].
 *
 * READ IT, the same as [SchemaResult]. A statement that fails doesn't throw: the other statements still run, and the
 * failure is recorded here. The short check is `failedStatements == 0`.
 */
data class ClusterSchemaResult(
    /**
     * Every node the schema ran on, each with its [SchemaResult].
     *
     * Empty when the schema didn't run: the cluster has no queues, or the same nodes and queues as at the last update
     * that went through.
     */
    val nodes: Map<NodeId, SchemaResult>
) {

    /**
     * How many statements failed, on all nodes together. `0` when everything went through, or the schema didn't run.
     */
    val failedStatements: Int
        get() = nodes.values.sumOf { it.failedStatements }

    /**
     * The nodes where some statement failed, each with its [SchemaResult]. Empty when nothing failed.
     */
    val failedNodes: Map<NodeId, SchemaResult>
        get() = nodes.filterValues { it.failedStatements > 0 }
}
