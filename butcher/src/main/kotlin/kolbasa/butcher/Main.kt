package kolbasa.butcher

import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.versionOption
import java.lang.invoke.MethodHandles

fun main(args: Array<String>) = ButcherCli()
    .subcommands(
        CheckCluster(),
        RebalanceCluster(),
        DrainNodes(),
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
    )
    .main(args)

/**
 * The `butcher` command itself, which does nothing but hold the real commands
 */
internal class ButcherCli : NoOpCliktCommand(name = "java -jar butcher.jar") {

    init {
        versionOption(butcherVersion(), message = { it })
        context {
            // The only thing it changes is that the commands are listed in sections, see HelpSection
            helpFormatter = { ButcherHelpFormatter(it) }
        }
    }

    override fun help(context: com.github.ajalt.clikt.core.Context) =
        "Inspect and manage a kolbasa cluster."

    override fun helpEpilog(context: com.github.ajalt.clikt.core.Context) = COMMAND_LEVELS_HELP
}

private val COMMAND_LEVELS_HELP = """
    A high-level command is built from the low-level ones and nothing else: it works out the same
    shard-balance plan that 'check-cluster' reports, then prepares, moves and finalizes a batch of shards
    at a time until nothing is left to move. The low-level commands stay yours to drive directly - for
    choosing the shards yourself, for moving only some tables, or for finishing a migration a high-level
    command refuses to touch.

    See https://github.com/vgv/kolbasa/blob/main/docs/Butcher.md for the full description.
""".trimIndent()

/**
 * The version of the jar this code was loaded from
 *
 * `MethodHandles.lookup().lookupClass()` is the idiomatic JVM way to get a Class handle to "the class
 * containing this code" without declaring a placeholder type. It resolves to the synthetic `MainKt` class
 * that Kotlin generates for the top-level functions of this file, whose `Package` carries the jar
 * manifest's `Implementation-Version` attribute set by the butcherJar task. Falls back to "unknown" outside
 * a jar (an IDE, `./gradlew run`), where the JVM does not populate the attribute.
 */
private fun butcherVersion(): String {
    return MethodHandles.lookup().lookupClass().`package`.implementationVersion ?: "unknown"
}
