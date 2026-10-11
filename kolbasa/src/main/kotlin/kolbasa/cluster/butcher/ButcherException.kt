package kolbasa.cluster.butcher

import kolbasa.producer.ShardId
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.Table

/**
 * Everything butcher refuses to do, as an exception with a message
 *
 * [message] is always present and is the text a human should see – the subclasses that carry structured
 * fields build it from them, so a caller that only logs the exception loses nothing.
 */
internal sealed class ButcherException(override val message: String) : RuntimeException(message) {

    internal class InvalidConfigurationException(message: String) : ButcherException(message)

    internal class ExecutionException(message: String) : ButcherException(message)

    internal class MoveToTheSameShardException(
        val shard: ShardId,
        val targetNode: NodeId
    ) : ButcherException(
        "Shard ${shard.id} is already both produced and consumed by node ${targetNode.id}, " +
            "there is nothing to move"
    )

    internal class MoveToNonExistingNodeException(
        val knownNodes: List<Node>,
        val targetNode: NodeId
    ) : ButcherException(
        "Target node ${targetNode.id} is not a member of this cluster. Known nodes: " +
            knownNodes.joinToString(separator = ", ") { it.id.id }
    )

    internal class InconsistentSchemaException(
        val tableName: String,
        val sourceSchema: Table,
        val targetSchema: Table
    ) : ButcherException(
        "Table '$tableName' has different schemas on different nodes of the cluster. Moving data between " +
            "nodes requires identical schemas, so fix the divergence first – 'check' reports it in detail"
    )
}
