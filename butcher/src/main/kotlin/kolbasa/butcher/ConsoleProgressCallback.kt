package kolbasa.butcher

import com.github.ajalt.mordant.animation.textAnimation
import com.github.ajalt.mordant.terminal.Terminal
import kolbasa.cluster.admin.ProgressCallback
import kolbasa.cluster.admin.ShardDiff
import kolbasa.cluster.admin.ShardOwnership
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import java.util.*
import javax.sql.DataSource

/**
 * Progress of a migration step run by hand
 *
 * The operator chose the shards and wants to see them, so prepare and finalize print a line per shard.
 * A rebalance prints counts instead, see [RebalanceProgressCallback].
 */
internal class ConsoleProgressCallback(private val terminal: Terminal) : ProgressCallback {

    private val progressLine = if (terminal.terminalInfo.interactive) {
        terminal.textAnimation<String> { it }
    } else {
        null
    }

    override fun prepareSuccessful(shards: List<ShardId>, targetNode: NodeId, shardsDiff: List<ShardDiff>) {
        terminal.println("Prepare")
        terminal.println("Shards: ${shards.map { it.id }}")
        terminal.println("Target node: ${targetNode.id}")
        terminal.println("Shards prepared to move (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            terminal.println("\t${diff.originalShard.shard}: ${describe(diff.originalShard)} => ${describe(diff.updatedShard)}")
        }
    }

    override fun moveStart(tableName: String, from: DataSource, to: DataSource) {
        terminal.println("Move table '$tableName' from $from to $to")
    }

    override fun moveNextBatch(
        tableName: String,
        from: DataSource,
        to: DataSource,
        batchRowsMoved: Int,
        tableRowsMoved: Long,
        totalRowsMoved: Long
    ) {
        if (progressLine != null) {
            progressLine.update("$tableName: moved ${count(tableRowsMoved)} rows, total: ${count(totalRowsMoved)} rows")
        } else {
            terminal.println(
                "$tableName: moved ${count(batchRowsMoved.toLong())} rows, table: ${count(tableRowsMoved)} rows, total: ${
                    count(
                        totalRowsMoved
                    )
                } rows"
            )
        }
    }

    override fun moveEnd(
        tableName: String,
        from: DataSource,
        to: DataSource,
        tableRowsMoved: Long
    ) {
        // The running line has said its last word for this table; the summary below is the permanent record
        progressLine?.clear()
        terminal.println(
            "Move table '$tableName' from $from to $to finished, migrated rows: ${count(tableRowsMoved)}"
        )
    }

    override fun finalizeSuccessful(shardsDiff: List<ShardDiff>) {
        terminal.println("Finalize")
        terminal.println("Shards moved to stable state (${shardsDiff.size}):")
        shardsDiff.forEach { diff ->
            terminal.println("\t${diff.originalShard.shard}: ${describe(diff.originalShard)} => ${describe(diff.updatedShard)}")
        }
    }

    private fun describe(ownership: ShardOwnership): String = when (ownership) {
        is ShardOwnership.Stable -> "stable on ${ownership.node.id}"
        is ShardOwnership.Migrating -> "migrating to ${ownership.target.id}"
    }
}

/** Group digits */
internal fun count(value: Long): String = String.format(Locale.ROOT, "%,d", value)
