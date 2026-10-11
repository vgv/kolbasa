package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.Context

internal class FinalizeMigration : ClusterCommand(name = "finalize-migration") {

    override fun help(context: Context) =
        "Complete a migration: hand the migrated shards over to their new node and restore consumer routing"

    override fun execute(): String? {
        butcher().finalizeMigration(ConsoleProgressCallback)
        return null
    }
}
