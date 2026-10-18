package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.IdSchema
import kolbasa.schema.NodeId

/**
 * Everything butcher refuses to do, as an exception with a message
 *
 * [message] is always present and is the text a human should see – the subclasses that carry structured
 * fields build it from them, so a caller that only logs the exception loses nothing.
 */
sealed class ClusterOperationException(override val message: String) : RuntimeException(message) {

    class ExecutionException(message: String) : ClusterOperationException(message)

    class MoveToTheSameShardException(
        val shard: ShardId,
        val targetNode: NodeId
    ) : ClusterOperationException(
        "Shard ${shard.id} is already both produced and consumed by node ${targetNode.id}, " +
            "there is nothing to move"
    )

    class MoveToNonExistingNodeException(
        val knownNodes: List<NodeId>,
        val targetNode: NodeId
    ) : ClusterOperationException(
        "Target node ${targetNode.id} is not a member of this cluster. Known nodes: " +
            knownNodes.joinToString(separator = ", ") { it.id }
    )

    class MigrationAlreadyInFlightException(
        val migratingShardsByTarget: Map<NodeId, List<ShardId>>
    ) : ClusterOperationException(
        "${migratingShardsByTarget.values.sumOf { it.size }} shard(s) are already migrating, to " +
            migratingShardsByTarget.keys.sortedBy { it.id }.joinToString(separator = ", ") { it.id } +
            ". A rebalance moves and finalizes every migrating shard, not only the ones it starts itself, " +
            "so it will not run on top of a migration that is already in flight: finish that one first – " +
            "move the data, then finalize – and run the rebalance again"
    )

    class DrainUnknownNodeException(
        val knownNodes: List<NodeId>,
        val nodes: List<NodeId>
    ) : ClusterOperationException(
        "Node(s) ${nodes.joinToString(separator = ", ") { it.id }} cannot be drained, they are not members " +
            "of this cluster. Known nodes: " + knownNodes.joinToString(separator = ", ") { it.id }
    )

    class DrainLastNodeException(
        val nodes: List<NodeId>
    ) : ClusterOperationException(
        "Draining ${nodes.joinToString(separator = ", ") { it.id }} would leave this cluster without a node " +
            "to hold the shards. Add a node to move them to, or drop the queues instead"
    )

    class InconsistentSchemaException(
        val tableName: String
    ) : ClusterOperationException(
        "Table '$tableName' has different schemas on different nodes of the cluster. Moving data between " +
            "nodes requires identical schemas, so fix the divergence first – 'check' reports it in detail"
    )

    class NodeIdMismatchException(
        val mismatches: Map<NodeId, NodeId>,
        val nodesWithoutId: List<NodeId>
    ) : ClusterOperationException(
        "Node ids don't match:\n" +
            (mismatches.toSortedMap().map { (expected, actual) -> "  ${expected.id}: this node's id is ${actual.id}" } +
                nodesWithoutId.sorted().map { expected ->
                    "  ${expected.id}: ${IdSchema.NODE_TABLE_NAME} has no active row. If the node holds no data yet, " +
                        "drop ${IdSchema.NODE_TABLE_NAME} and run again. Otherwise, restore the row by hand"
                }).joinToString(separator = "\n")
    )
}
