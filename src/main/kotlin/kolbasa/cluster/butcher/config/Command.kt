package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import kolbasa.schema.NodeId

internal sealed class Command(val nodes: ClusterNodes, val command: AvailableCommand) {

    class Check(nodes: ClusterNodes, val checks: Set<AvailableCheck>) : Command(nodes, AvailableCommand.CHECK_CLUSTER)

    class Prepare(nodes: ClusterNodes, val target: NodeId, val shards: List<Int>) :
        Command(nodes, AvailableCommand.PREPARE_MIGRATION)

    /**
     * Transfer data to target nodes. Safe to re-run if crashed — INSERT uses ON CONFLICT DO NOTHING.
     */
    class Move(nodes: ClusterNodes, val includeTables: Set<String>?, val excludeTables: Set<String>?) :
        Command(nodes, AvailableCommand.MOVE_DATA)

    class Finalize(nodes: ClusterNodes) : Command(nodes, AvailableCommand.FINALIZE_MIGRATION)

    companion object {
        internal fun parseCommand(args: Array<String>): Command {
            if (args.isEmpty()) {
                throw ButcherException.InvalidConfigurationException(AvailableCommand.globalUsage())
            }

            val commandName = args[0]
            val command = AvailableCommand.fromCommandName(commandName)
            if (command == null) {
                val msg = "Unknown command: '$commandName'\n${AvailableCommand.globalUsage()}"
                throw ButcherException.InvalidConfigurationException(msg)
            }

            return command.parse(args)
        }
    }
}



