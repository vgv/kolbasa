package kolbasa.cluster.butcher

import kolbasa.cluster.butcher.config.Command
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val command = try {
        Command.parseCommand(args)
    } catch (e: ButcherException.InvalidConfigurationException) {
        println("=================================================")
        println("Invalid configuration.")
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
        println("Execution error.")
        println(e.messageToShow)
        println("=================================================")
        exitProcess(1)
    }

    println("=================================================")
    println("Completed successfully.")
    println(result)
    println("=================================================")
}
