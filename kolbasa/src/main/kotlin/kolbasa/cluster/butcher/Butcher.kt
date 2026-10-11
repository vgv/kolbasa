package kolbasa.cluster.butcher

import kolbasa.cluster.butcher.check.CheckResult
import kolbasa.cluster.butcher.check.MigrationState
import kolbasa.cluster.butcher.check.MigrationStateResult
import kolbasa.cluster.butcher.check.OrphanTables
import kolbasa.cluster.butcher.check.OrphanTablesResult
import kolbasa.cluster.butcher.check.SchemaConsistency
import kolbasa.cluster.butcher.check.SchemaConsistencyResult
import kolbasa.cluster.butcher.check.ShardBalance
import kolbasa.cluster.butcher.check.ShardBalanceResult
import kolbasa.cluster.butcher.check.render
import kolbasa.cluster.butcher.config.AvailableCheck
import kolbasa.cluster.butcher.config.Command
import java.lang.invoke.MethodHandles
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.isNotEmpty() && (args[0] == "--version" || args[0] == "-v")) {
        println(butcherVersion())
        exitProcess(0)
    }

    val command = try {
        Command.parseCommand(args)
    } catch (e: ButcherException.InvalidConfigurationException) {
        println("=================================================")
        println(red("Invalid configuration."))
        println(e.message)
        println("=================================================")
        exitProcess(1)
    }

    val result = try {
        when (command) {
            is Command.Check -> runChecks(command)

            is Command.Prepare -> prepare(
                dataSources = command.nodes.dataSources,
                target = command.target,
                shards = command.shards,
                progressCallback = ConsoleProgressCallback
            )

            is Command.Move -> move(
                dataSources = command.nodes.dataSources,
                includeTables = command.includeTables,
                excludeTables = command.excludeTables,
                progressCallback = ConsoleProgressCallback
            )

            is Command.Finalize -> finalize(
                dataSources = command.nodes.dataSources,
                progressCallback = ConsoleProgressCallback
            )
        }
    } catch (e: ButcherException.ExecutionException) {
        println("=================================================")
        println(red("Execution error."))
        println(e.message)
        println("=================================================")
        exitProcess(1)
    }

    println("=================================================")
    println(green("Completed successfully."))
    if (result is CheckResult) {
        println(result.render())
    }
    println("=================================================")
}

/**
 * Runs the checks the command asked for. Which checks exist and how they are named on the command line is a
 * property of the CLI, so the mapping lives here and not in the library: every check is callable on its own.
 */
private fun runChecks(command: Command.Check): CheckResult {
    val dataSources = command.nodes.dataSources
    MoveHelpers.checkClusterNodes(dataSources)

    var shardBalance: ShardBalanceResult? = null
    var schemaConsistency: SchemaConsistencyResult? = null
    var orphanTables: OrphanTablesResult? = null
    var migrationState: MigrationStateResult? = null

    command.checks.forEach { check ->
        when (check) {
            AvailableCheck.CHECK_SHARD_BALANCE -> shardBalance = ShardBalance.check(dataSources)
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> schemaConsistency = SchemaConsistency.check(dataSources)
            AvailableCheck.FIND_ORPHAN_TABLES -> orphanTables = OrphanTables.check(dataSources)
            AvailableCheck.CHECK_MIGRATION_STATE -> migrationState = MigrationState.check(dataSources)
        }
    }

    return CheckResult(
        shardBalance = shardBalance,
        schemaConsistency = schemaConsistency,
        orphanTables = orphanTables,
        migrationState = migrationState
    )
}

private fun butcherVersion(): String {
    // `MethodHandles.lookup().lookupClass()` is the idiomatic JVM way to get a
    // Class handle to "the class containing this code" without declaring a
    // placeholder type. It resolves to the synthetic `ButcherKt` class that
    // Kotlin generates for top-level functions in this file, whose `Package`
    // carries the jar manifest's `Implementation-Version` attribute set by
    // the shadow task. Falls back to "unknown" outside a jar (IDE,
    // `./gradlew run`), where the JVM doesn't populate the attribute.
    return MethodHandles.lookup().lookupClass().`package`.implementationVersion ?: "unknown"
}

private fun red(msg: String): String {
    return "$ANSI_RED$msg$ANSI_RESET"
}

private fun green(msg: String): String {
    return "$ANSI_GREEN$msg$ANSI_RESET"
}

private const val ANSI_RESET = "\u001B[0m"
private const val ANSI_RED = "\u001B[31m"
private const val ANSI_GREEN = "\u001B[32m"
