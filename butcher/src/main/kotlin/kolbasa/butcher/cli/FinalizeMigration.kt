package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.clikt.core.terminal

internal class FinalizeMigration : MutatingClusterCommand(name = "finalize-migration") {

    override fun help(context: Context) =
        "Complete a migration: hand the migrated shards over to their new node and restore consumer routing"

    override fun execute(): Widget? {
        confirm("Every shard that is currently migrating will be handed over to its new node, and consumers will stop reading it from the old one. Run this only once move-data has nothing left to copy.")

        butcher().finalizeMigration(ConsoleProgressCallback(terminal))
        return null
    }
}
