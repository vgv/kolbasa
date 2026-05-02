package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import kolbasa.schema.NodeId

/**
 * All CLI commands the butcher tool supports. Holds the command name plus
 * help texts so that everything about a command lives in one place.
 *
 * @param commandName subcommand name as typed on the CLI, e.g. "check-cluster"
 * @param shortUsage short one-line description used in the global help listing
 * @param fullUsage multi-line detailed help printed when the user runs the command without enough args
 */
internal enum class AvailableCommand(
    val commandName: String,
    val shortUsage: String,
    val fullUsage: String,
) {

    CHECK_CLUSTER(
        commandName = "check-cluster",
        shortUsage = "Inspect cluster state (schema consistency, shards distribution etc.)",
        fullUsage = """
            Usage: java -jar butcher.jar check-cluster [<check-name>] <config-file>...

            Inspect cluster state. Does not modify anything. Can be run at any time.
            If <check-name> is omitted, all checks are run.

            Available checks:
              ${AvailableCheck.CHECK_SHARD_BALANCE.usageLine()}
              ${AvailableCheck.CHECK_SCHEMA_CONSISTENCY.usageLine()}
              ${AvailableCheck.FIND_ORPHAN_TABLES.usageLine()}
              ${AvailableCheck.CHECK_MIGRATION_STATE.usageLine()}
              ${AvailableCheck.usageLine("all", "Run all checks")}

            $CONFIG_FILE_FORMAT_HELP
        """.trimIndent()
    ) {
        // check-cluster <config-file>...                => all checks
        // check-cluster <check-name> <config-file>...   => specific check
        override fun parse(args: Array<String>): Command.Check {
            val tail = args.drop(1)

            val (checks, restArgs) = if (tail.isNotEmpty()) {
                val candidate = tail[0]
                when {
                    candidate == "all" -> AvailableCheck.all to tail.drop(1)
                    AvailableCheck.fromCheckName(candidate) != null -> setOf(AvailableCheck.fromCheckName(candidate)!!) to tail.drop(
                        1
                    )

                    else -> AvailableCheck.all to tail
                }
            } else {
                AvailableCheck.all to emptyList()
            }

            val parsed = try {
                ConfigHelper.parseArgs(restArgs, supportedFlags = emptySet())
            } catch (e: ButcherException.InvalidConfigurationException) {
                throw wrapWithUsage(e)
            }

            val nodes = ClusterNodes.buildClusterNodes(parsed.files)
            return Command.Check(nodes, checks)
        }
    },

    PREPARE_MIGRATION(
        commandName = "prepare-migration", shortUsage = "Mark shards for migration", fullUsage = """
            Usage: java -jar butcher.jar prepare-migration --target=<node-id> --shards=<0,1,2,...> <config-file>...

            Mark shards for migration to a target node. Updates shard metadata only,
            does not move data. Run 'move-data' after this step.

            Required flags:
              --target=<node-id>         target node identifier (must match an id in config)
              --shards=<0,1,2,...>       comma-separated shard numbers

            $CONFIG_FILE_FORMAT_HELP
        """.trimIndent()
    ) {
        override fun parse(args: Array<String>): Command.Prepare {
            val parsed = try {
                ConfigHelper.parseArgs(args.drop(1), supportedFlags = setOf(FLAG_TARGET, FLAG_SHARDS))
            } catch (e: ButcherException.InvalidConfigurationException) {
                throw wrapWithUsage(e)
            }

            val target = parsed.flags[FLAG_TARGET]
                ?: throw ButcherException.InvalidConfigurationException("'${FLAG_TARGET}=<node-id>' is required for $commandName\n$fullUsage")

            val shardsRaw = parsed.flags[FLAG_SHARDS]
                ?: throw ButcherException.InvalidConfigurationException("'${FLAG_SHARDS}=<0,1,2,...>' is required for $commandName\n$fullUsage")

            val shards = shardsRaw.split(",").map {
                val trimmed = it.trim()
                trimmed.toIntOrNull() ?: throw ButcherException.InvalidConfigurationException(
                    "Invalid shard number '$trimmed' in '${FLAG_SHARDS}'"
                )
            }

            val nodes = ClusterNodes.buildClusterNodes(parsed.files)
            return Command.Prepare(nodes, NodeId(target), shards)
        }
    },

    MOVE_DATA(
        commandName = "move-data", shortUsage = "Transfer data to target nodes (safe to re-run if crashed)", fullUsage = """
            Usage: java -jar butcher.jar move-data [--tables=<t1,t2,...>] <config-file>...

            Transfer data from source nodes to target nodes for all shards in migration state.
            Safe to re-run — INSERT uses ON CONFLICT DO NOTHING.

            Optional flags:
              --tables=<t1,t2,...>       comma-separated queue table names (default: all)

            $CONFIG_FILE_FORMAT_HELP
        """.trimIndent()
    ) {
        override fun parse(args: Array<String>): Command.Move {
            val parsed = try {
                ConfigHelper.parseArgs(args.drop(1), supportedFlags = setOf(FLAG_TABLES))
            } catch (e: ButcherException.InvalidConfigurationException) {
                throw wrapWithUsage(e)
            }

            val tables = parsed.flags[FLAG_TABLES]?.split(",")?.map { it.trim() }?.toSet()
            val nodes = ClusterNodes.buildClusterNodes(parsed.files)
            return Command.Move(nodes, tables)
        }
    },

    FINALIZE_MIGRATION(
        commandName = "finalize-migration", shortUsage = "Complete migration, restore consumer routing", fullUsage = """
            Usage: java -jar butcher.jar finalize-migration <config-file>...

            Complete migration: restore consumer routing for migrated shards.
            Run after 'move-data' has finished and target node has been verified.

            $CONFIG_FILE_FORMAT_HELP
        """.trimIndent()
    ) {
        override fun parse(args: Array<String>): Command.Finalize {
            val parsed = try {
                ConfigHelper.parseArgs(args.drop(1), supportedFlags = emptySet())
            } catch (e: ButcherException.InvalidConfigurationException) {
                throw wrapWithUsage(e)
            }

            val nodes = ClusterNodes.buildClusterNodes(parsed.files)
            return Command.Finalize(nodes)
        }
    };

    /** Build the typed [Command] from command-line args (everything after the command name). */
    abstract fun parse(args: Array<String>): Command

    /** Wrap a parse error with this command's full usage appended. */
    protected fun wrapWithUsage(e: ButcherException.InvalidConfigurationException): ButcherException.InvalidConfigurationException {
        return ButcherException.InvalidConfigurationException("${e.messageToShow}\n\n$fullUsage")
    }

    companion object {
        init {
            val duplicates = entries.groupBy { it.commandName }.filterValues { it.size > 1 }.keys
            check(duplicates.isEmpty()) {
                "Duplicate command names in AvailableCommand: $duplicates"
            }
        }

        fun fromCommandName(name: String): AvailableCommand? = entries.find { it.commandName == name }

        fun globalUsage(): String = buildString {
            appendLine("Usage: java -jar butcher.jar <command> [flags] <config-file>...")
            appendLine()
            appendLine("Commands:")
            val width = entries.maxOf { it.commandName.length }
            entries.forEach { cmd ->
                appendLine("  ${cmd.commandName.padEnd(width)}  ${cmd.shortUsage}")
            }
            appendLine()
            append("Run 'java -jar butcher.jar <command>' for command-specific help.")
        }
    }
}

private val CONFIG_FILE_FORMAT_HELP = """
            Config file format (one or more files, merged by id):
              <node-id>  host=<host> port=<port> user=<user> password=<pwd> dbname=<db> [schema=<schema>]

            Example:
              node-01  host=db01.internal port=5432 user=app dbname=orders schema=public
              node-02  host=db02.internal port=5432 user=app dbname=orders schema=public

            - Multiple files are merged by node id (e.g. split topology and secrets).
            - Supported keys: host, port, user, password, dbname, schema. Unknown keys ignored.
            - Values can be single-quoted for whitespace or special characters.
            - Lines starting with '#' and blank lines are ignored.
        """.trimIndent()

private const val FLAG_TARGET = "--target"
private const val FLAG_SHARDS = "--shards"
private const val FLAG_TABLES = "--tables"



