package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.ConsoleProgressCallback
import kolbasa.butcher.HelpSection

internal class FinalizeMigration : MutatingClusterCommand(name = "finalize-migration") {

    override val helpSection = HelpSection.LOW_LEVEL

    override fun help(context: Context) =
        "Finish a migration. Migrated shards switch to their new node, and consumers read them there"

    override fun execute(): Widget? {
        confirm("All migrating shards will switch to their new node. Consumers will read them only from there. " +
            "Run this only when move-data has nothing left to copy.")

        butcher().finalizeMigration(ConsoleProgressCallback(terminal))
        return null
    }
}
