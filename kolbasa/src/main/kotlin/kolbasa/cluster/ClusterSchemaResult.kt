package kolbasa.cluster

import kolbasa.schema.NodeId
import kolbasa.schema.SchemaResult

/**
 * What the schema update of a cluster did on every node, see [Cluster.updateTopology].
 *
 * READ IT, the same as [SchemaResult]. A statement that fails doesn't throw: the other statements still run, and the
 * failure is recorded here. The short check is `failedStatements == 0`.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val result = cluster.updateTopology()
 *
 * if (result.failedStatements > 0) {
 *     result.failedNodes.forEach { (node, nodeResult) ->
 *         (nodeResult.failedTableStatements + nodeResult.failedIndexStatements).forEach {
 *             println("${node.id}: ${it.statement} -> ${it.error}")
 *         }
 *     }
 * }
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var result = cluster.updateTopology();
 *
 * if (result.getFailedStatements() > 0) {
 *     result.getFailedNodes().forEach((node, nodeResult) -> {
 *         var failed = new ArrayList<>(nodeResult.getFailedTableStatements());
 *         failed.addAll(nodeResult.getFailedIndexStatements());
 *         failed.forEach(f -> System.out.println(node.getId() + ": " + f.getStatement() + " -> " + f.getError()));
 *     });
 * }
 * ```
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
    val failedStatements: Int = nodes.values.sumOf { it.failedStatements }

    /**
     * The nodes where some statement failed, each with its [SchemaResult]. Empty when nothing failed.
     */
    val failedNodes: Map<NodeId, SchemaResult>
        get() = nodes.filterValues { it.failedStatements > 0 }
}
