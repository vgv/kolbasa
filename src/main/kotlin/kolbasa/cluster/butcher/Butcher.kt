package kolbasa.cluster.butcher

import kolbasa.cluster.butcher.check.check
import kolbasa.cluster.butcher.config.Command
import kotlin.system.exitProcess

fun main(args: Array<String>) {
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

private fun red(msg: String): String {
    return "$ANSI_RED$msg$ANSI_RESET"
}

private fun green(msg: String): String {
    return "$ANSI_GREEN$msg$ANSI_RESET"
}

private const val ANSI_RESET = "\u001B[0m"
private const val ANSI_RED = "\u001B[31m"
private const val ANSI_GREEN = "\u001B[32m"
