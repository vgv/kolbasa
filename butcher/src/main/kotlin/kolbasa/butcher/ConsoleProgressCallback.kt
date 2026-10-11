package kolbasa.butcher

import com.github.ajalt.mordant.terminal.Terminal
import kolbasa.cluster.admin.ProgressCallback
import kolbasa.cluster.admin.ShardDiff
import kolbasa.cluster.admin.ShardOwnership
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

internal class ConsoleProgressCallback(private val terminal: Terminal) : ProgressCallback {

    override fun prepareSuccessful(shards: List<ShardId>, targetNode: NodeId, shardsDiff: List<ShardDiff>) {
        terminal.println("Prepare")
        terminal.println("Shards: ${shards.map { it.id }}")
        terminal.println("Target node: ${targetNode.id}")
        terminal.println("Shards prepared to move (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            terminal.println("\t${diff.originalShard.shard}: ${describe(diff.originalShard)} => ${describe(diff.updatedShard)}")
        }
    }

    private fun describe(ownership: ShardOwnership): String = when (ownership) {
        is ShardOwnership.Stable -> "stable on ${ownership.node.id}"
        is ShardOwnership.Migrating -> "migrating to ${ownership.target.id}"
    }

    override fun moveStart(tableName: String, from: DataSource, to: DataSource) {
        terminal.println("Move table '$tableName' from $from to $to")
    }

    override fun moveEnd(
        tableName: String,
        from: DataSource,
        to: DataSource,
        migratedRows: Int
    ) {
        terminal.println("Move table '$tableName' from $from to $to finished, migrated rows: $migratedRows")
    }

    override fun moveNextBatch(
        tableName: String,
        from: DataSource,
        to: DataSource,
        migratedRows: Int
    ) {
        // One line per batch of 1000 rows is something to watch on a terminal and something to drown in a
        // log file, so a redirected run gets the start and the end of each table only
        if (terminal.terminalInfo.outputInteractive) {
            terminal.println("Move table '$tableName' from $from to $to, migrated rows: $migratedRows")
        }
    }

    override fun finalizeSuccessful(shardsDiff: List<ShardDiff>) {
        terminal.println("Finalize")
        terminal.println("Shards moved to stable state (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            terminal.println("\t${diff.originalShard.shard}: ${describe(diff.originalShard)} => ${describe(diff.updatedShard)}")
        }
    }
}
