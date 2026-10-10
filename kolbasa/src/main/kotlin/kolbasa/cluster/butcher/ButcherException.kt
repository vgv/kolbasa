package kolbasa.cluster.butcher

import kolbasa.cluster.Shard
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.Table

internal sealed class ButcherException : RuntimeException() {

    internal data class InvalidConfigurationException(
        val messageToShow: String
    ) : ButcherException()

    internal data class ExecutionException(
        val messageToShow: String
    ) : ButcherException()

    internal data class MoveToTheSameShardException(
        val shard: Shard,
        val targetNode: NodeId
    ) : ButcherException()

    internal data class MoveToNonExistingNodeException(
        val knownNodes: List<Node>,
        val targetNode: NodeId
    ) : ButcherException()

    internal data class InconsistentSchemaException(
        val tableName: String,
        val sourceSchema: Table,
        val targetSchema: Table
    ) : ButcherException()
}
