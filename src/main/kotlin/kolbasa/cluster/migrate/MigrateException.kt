package kolbasa.cluster.migrate

import kolbasa.cluster.Shard
import kolbasa.schema.Node
import kolbasa.schema.Table

internal sealed class MigrateException : RuntimeException() {

    internal class MigrateToTheSameShardException(
        val shard: Shard,
        val targetNode: String
    ) : MigrateException()

    internal class MigrateToNonExistingNodeException(
        val knownNodes: List<Node>,
        val targetNode: String
    ) : MigrateException()

    internal class InconsistentSchemaException(
        val tableName: String,
        val sourceSchema: Table,
        val targetSchema: Table
    ) : MigrateException()
}
