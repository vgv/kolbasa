package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.check
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.clikt.parameters.types.int
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

internal class PrepareMigration : MutatingClusterCommand(name = "prepare-migration") {

    private val target by option("--target", metavar = "<node-id>")
        .convert { NodeId(it) }
        .required()
        .help("Target node identifier, which must match an id in the config")

    private val shards by option("--shards", metavar = "<0,1,2,...>")
        .int()
        .split(",")
        .required()
        .help("Comma-separated shard numbers to move")
        // Validated here and not by ShardId.of(), which folds anything into the range: a shard the operator
        // typed by hand is user input, and 9999 is a mistake rather than shard 783
        .check({ out -> "shard(s) out of range [${ShardId.MIN_SHARD}..${ShardId.MAX_SHARD}]: " +
            out.filterNot { it in ShardId.SHARDS_RANGE }.joinToString(", ") }) { shards ->
            shards.all { it in ShardId.SHARDS_RANGE }
        }

    override fun help(context: Context) =
        "Mark shards for migration. Updates shard metadata only, run 'move-data' to start the migration"

    override fun execute(): Widget? {
        confirm("Shards ${shards.joinToString(",")} will be marked for migration to node ${target.id}, and new messages for them will go there immediately.")

        butcher().prepareMigration(target, shards.map(ShardId::of), ConsoleProgressCallback(terminal))
        return null
    }
}
