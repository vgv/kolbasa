package kolbasa.cluster.butcher

import kolbasa.cluster.butcher.check.check
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
        println(e.messageToShow)
        println("=================================================")
        exitProcess(1)
    }

    val result = try {
        when (command) {
            is Command.Check -> check(command)
            is Command.Prepare -> prepare(command)
            is Command.Move -> move(command)
            is Command.Finalize -> finalize(command)
        }
    } catch (e: ButcherException.ExecutionException) {
        println("=================================================")
        println(red("Execution error."))
        println(e.messageToShow)
        println("=================================================")
        exitProcess(1)
    }

    println("=================================================")
    println(green("Completed successfully."))
    println(result)
    println("=================================================")
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
