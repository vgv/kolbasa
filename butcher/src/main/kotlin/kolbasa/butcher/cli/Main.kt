package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.versionOption
import java.lang.invoke.MethodHandles

fun main(args: Array<String>) = ButcherCli()
    .subcommands(
        CheckCluster(),
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
    )
    .main(args)

/**
 * The `butcher` command itself, which does nothing but hold the four real commands
 */
internal class ButcherCli : NoOpCliktCommand(name = "java -jar butcher.jar") {

    init {
        versionOption(butcherVersion(), message = { it })
    }

    override fun help(context: com.github.ajalt.clikt.core.Context) =
        "Inspect and manage a kolbasa cluster."
}

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
