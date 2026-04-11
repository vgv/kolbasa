package kolbasa.cluster.butcher

import kolbasa.schema.NodeId
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.net.URI
import javax.sql.DataSource

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
            Usage: java -jar butcher.jar check-cluster [<check-name>] <config-file>

            Inspect cluster state. Does not modify anything. Can be run at any time.
            If <check-name> is omitted, all checks are run.

            Available checks:
              ${AvailableCheck.CHECK_SHARD_BALANCE.usageLine()}
              ${AvailableCheck.CHECK_SCHEMA_CONSISTENCY.usageLine()}
              ${AvailableCheck.FIND_ORPHAN_TABLES.usageLine()}
              ${AvailableCheck.CHECK_MIGRATION_STATE.usageLine()}
              ${AvailableCheck.usageLine("all", "Run all checks")}

            Required config keys:
              $NODE_FORMAT
        """.trimIndent()
    ) {
        // check-cluster <config-file>              => all checks
        // check-cluster <check-name> <config-file> => specific check
        override fun parse(args: Array<String>): Command.Check {
            val configFilePath: String
            val checks: Set<AvailableCheck>

            when (args.size) {
                2 -> {
                    // check-cluster <config-file>
                    configFilePath = args[1]
                    checks = AvailableCheck.all()
                }

                3 -> {
                    // check-cluster <check-name> <config-file>
                    val checkName = args[1]
                    checks = when (checkName) {
                        "all" -> AvailableCheck.all()
                        else -> {
                            val check = AvailableCheck.fromCheckName(checkName)
                            if (check == null) {
                                val errorMessage = buildString {
                                    appendLine("Unknown check: '$checkName'")
                                    appendLine(fullUsage)
                                }
                                throw ButcherException.InvalidConfigurationException(errorMessage)
                            }

                            setOf(check)
                        }
                    }

                    configFilePath = args[2]
                }

                else -> {
                    throw ButcherException.InvalidConfigurationException(fullUsage)
                }
            }

            val raw = parseRawConfig(File(configFilePath))
            val nodes = parseClusterNodes(raw)
            return Command.Check(nodes, checks)
        }
    },
    PREPARE_MIGRATION(
        commandName = "prepare-migration",
        shortUsage = "Mark shards for migration",
        fullUsage = """
            Usage: java -jar butcher.jar prepare-migration <config-file>

            Mark shards for migration to a target node. Updates shard metadata only,
            does not move data. Run 'move-data' after this step.

            Required config keys:
              $NODE_FORMAT
              target = <node-id>                                                (target node identifier)
              shards = 0,1,2,3                                                  (comma-separated shard numbers)
        """.trimIndent()
    ) {
        override fun parse(args: Array<String>): Command.Prepare {
            if (args.size != 2) {
                throw ButcherException.InvalidConfigurationException(fullUsage)
            }

            val raw = parseRawConfig(File(args[1]))
            val nodes = parseClusterNodes(raw)

            val target = raw["target"]?.firstOrNull()
                ?: throw ButcherException.InvalidConfigurationException("'target' is required for $commandName")
            val shards = raw["shards"]?.firstOrNull()?.split(",")?.map { it.trim().toInt() }
                ?: throw ButcherException.InvalidConfigurationException("'shards' is required for $commandName")

            return Command.Prepare(nodes, NodeId(target), shards)
        }
    },
    MOVE_DATA(
        commandName = "move-data",
        shortUsage = "Transfer data to target nodes (safe to re-run if crashed)",
        fullUsage = """
            Usage: java -jar butcher.jar move-data <config-file>

            Transfer data from source nodes to target nodes for all shards in migration state.
            Can be re-run safely if interrupted — INSERT uses ON CONFLICT DO NOTHING.
            Run 'prepare-migration' first. Run 'finalize-migration' after this step.

            Required config keys:
              $NODE_FORMAT

            Optional config keys:
              tables = table1,table2                                            (default: all queue tables)
        """.trimIndent()
    ) {
        override fun parse(args: Array<String>): Command.Move {
            if (args.size != 2) {
                throw ButcherException.InvalidConfigurationException(fullUsage)
            }

            val raw = parseRawConfig(File(args[1]))
            val nodes = parseClusterNodes(raw)

            val tables = raw["tables"]?.firstOrNull()?.split(",")?.map { it.trim() }?.toSet()
            return Command.Move(nodes, tables)
        }
    },
    FINALIZE_MIGRATION(
        commandName = "finalize-migration",
        shortUsage = "Complete migration, restore consumer routing",
        fullUsage = """
            Usage: java -jar butcher.jar finalize-migration <config-file>

            Complete migration: restore consumer routing for migrated shards.
            Run after 'move-data' has finished and target node has been verified.

            Required config keys:
              $NODE_FORMAT
        """.trimIndent()
    ) {
        override fun parse(args: Array<String>): Command.Finalize {
            if (args.size != 2) {
                throw ButcherException.InvalidConfigurationException(fullUsage)
            }

            val raw = parseRawConfig(File(args[1]))
            val nodes = parseClusterNodes(raw)

            return Command.Finalize(nodes)
        }
    };

    /** Build the typed [Command] from command-line args (everything after the command name). */
    abstract fun parse(args: Array<String>): Command

    companion object {
        init {
            val duplicates = entries.groupBy { it.commandName }.filterValues { it.size > 1 }.keys
            check(duplicates.isEmpty()) {
                "Duplicate command names in AvailableCommand: $duplicates"
            }
        }

        fun fromCommandName(name: String): AvailableCommand? =
            entries.find { it.commandName == name }

        fun globalUsage(): String = buildString {
            appendLine("Usage: java -jar butcher.jar <command> [options] <config-file>")
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

/**
 * Available cluster checks that can be run independently or all at once.
 * @param checkName short CLI name used in `check-cluster <subcheck> <config-file>`
 */
internal enum class AvailableCheck(val checkName: String, val shortUsage: String) {
    /** Check shard balancing, advise about possible migration to distribute load evenly */
    CHECK_SHARD_BALANCE(
        checkName = "shard-balance",
        shortUsage = "Check shard balancing, advise about possible migration to distribute load evenly"
    ),

    /** Check that all queue tables exist on every node with the same structure (columns, indexes) */
    CHECK_SCHEMA_CONSISTENCY(
        checkName = "schema-consistency",
        shortUsage = "Check that all queue tables exist on every node with the same structure (columns, indexes)"
    ),

    /** Find orphan companion tables (DLQ and Archive) on some nodes but not others */
    FIND_ORPHAN_TABLES(
        checkName = "find-orphans",
        shortUsage = "Find orphan companion tables (DLQ and Archive) on some nodes but not others"
    ),

    /** Find shards stuck in migration state (consumerNode == null, nextConsumerNode != null) */
    CHECK_MIGRATION_STATE(
        checkName = "migration-state",
        shortUsage = "Find shards stuck in migration state"
    );

    fun usageLine(): String = AvailableCheck.usageLine(checkName, shortUsage)

    companion object {
        init {
            val duplicates = entries.groupBy { it.checkName }.filterValues { it.size > 1 }.keys
            check(duplicates.isEmpty()) {
                "Duplicate check names in AvailableCheck: $duplicates"
            }
        }

        fun fromCheckName(name: String): AvailableCheck? = entries.find { it.checkName == name }

        fun all(): Set<AvailableCheck> = entries.toSet()

        fun usageLine(name: String, usage: String): String {
            val defaultPadding = AvailableCheck.entries.maxOf { it.checkName.length } + 2
            return "${name.padEnd(defaultPadding)}$usage"
        }
    }
}

// ---------------------------------------------------------------------------
// Data classes
// ---------------------------------------------------------------------------

internal data class ClusterNodes(
    val dataSources: List<DataSource>
)

internal sealed class Command(val nodes: ClusterNodes, val command: AvailableCommand) {

    class Check(nodes: ClusterNodes, val checks: Set<AvailableCheck>) :
        Command(nodes, AvailableCommand.CHECK_CLUSTER)

    class Prepare(nodes: ClusterNodes, val target: NodeId, val shards: List<Int>) :
        Command(nodes, AvailableCommand.PREPARE_MIGRATION)

    /** Transfer data to target nodes. Safe to re-run if crashed — INSERT uses ON CONFLICT DO NOTHING. */
    class Move(nodes: ClusterNodes, val tables: Set<String>?) :
        Command(nodes, AvailableCommand.MOVE_DATA)

    class Finalize(nodes: ClusterNodes) :
        Command(nodes, AvailableCommand.FINALIZE_MIGRATION)
}

/**
 * Public entry point: parse args -> typed [Command].
 */
internal fun parseCommand(args: Array<String>): Command {
    if (args.isEmpty()) {
        // No args at all
        throw ButcherException.InvalidConfigurationException(AvailableCommand.globalUsage())
    }

    val commandName = args[0]
    val command = AvailableCommand.fromCommandName(commandName)

    if (command == null) {
        val errorMessage = buildString {
            appendLine("Unknown command: '$commandName'")
            appendLine(AvailableCommand.globalUsage())
        }

        throw ButcherException.InvalidConfigurationException(errorMessage)
    }

    return command.parse(args)
}

/**
 * Raw file parser: supports repeated keys, `#` comments, blank lines.
 */
internal fun parseRawConfig(file: File): Map<String, List<String>> {
    if (!file.canRead()) {
        throw ButcherException.InvalidConfigurationException("Cannot read config file: ${file.absolutePath}")
    }

    val result = mutableMapOf<String, MutableList<String>>()
    file.readLines()
        .map { it.trim() }
        .filter { it.isNotEmpty() && !it.startsWith("#") }
        .forEach { line ->
            val (key, value) = line.split("=", limit = 2).map { it.trim() }
            result.computeIfAbsent(key) { mutableListOf() }.add(value)
        }
    return result
}

/**
 * Cluster nodes: parse `node` entries into [DataSource]s.
 */
internal fun parseClusterNodes(raw: Map<String, List<String>>): ClusterNodes {
    val dbNodes = raw["node"]
    if (dbNodes == null) {
        val errorMessage = buildString {
            appendLine("Can't find any kolbasa cluster nodes.")
            appendLine("At least one 'node' entry is required in config file. Format: ")
            append("  ").appendLine(NODE_FORMAT)
        }
        throw ButcherException.InvalidConfigurationException(errorMessage)
    }

    val dataSources = dbNodes.map { createDataSource(it) }
    return ClusterNodes(dataSources)
}

/**
 * Parsed `postgresql://` endpoint, split into all components required to
 * build a [DataSource].
 *
 * @param host PostgreSQL host
 * @param port PostgreSQL port, or `null` if not specified in the URL
 * @param dbName database name (path segment after `/`)
 * @param user user name extracted from the URI's userInfo, or `null` if absent
 * @param password password extracted from the URI's userInfo, or `null` if absent
 * @param schema optional schema name taken from extra `schema=...` param
 * @param urlProperties connection properties extracted from the URI's query string
 *   (e.g. `sslmode=require`)
 */
internal data class PgEndpoint(
    val host: String,
    val port: Int?,
    val dbName: String,
    val user: String?,
    val password: String?,
    val schema: String?,
    val urlProperties: Map<String, String>,
) {
    /** Human-readable identifier: only host, port and database are shown — no credentials. */
    override fun toString(): String = buildString {
        append(host)
        append(":").append(port ?: 5432)
        append("/").append(dbName)
    }
}

/**
 * Parse `postgresql://user:pwd@host:5432/db,schema=myschema` into a [PgEndpoint].
 *
 * Format: standard `postgresql://` URI, optionally followed by comma-separated
 * `key=value` params.
 */
private fun parsePgEndpoint(pgUrl: String): PgEndpoint {
    // postgresql://user:pwd@host:5432/db,schema=myschema -> ["postgresql://user:pwd@host:5432/db", "schema=myschema"]
    val parts = pgUrl.split(",")
    val postgresqlUrl = parts[0]

    // Parse optional extra params, like "schema=NNN,socketTimeout=123"
    val extraParams = mutableMapOf<String, String>()
    for (i in 1 until parts.size) {
        val pair = parts[i].trim().split("=", limit = 2)
        if (pair.size == 2) {
            extraParams[pair[0]] = pair[1]
        }
    }

    // Parse the postgresql:// URI with java.net.URI
    val uri = URI(postgresqlUrl)

    // Host is required, port is optional (-1 means "not specified")
    val host = uri.host ?: throw ButcherException.InvalidConfigurationException("Host is required in '$postgresqlUrl'")
    val port = uri.port.takeIf { it != -1 }

    // Database name is the path segment, without the leading '/'
    val dbName = uri.path?.removePrefix("/")?.takeIf { it.isNotEmpty() }
        ?: throw ButcherException.InvalidConfigurationException("Database name is required in '$postgresqlUrl'")

    // Extract user and password from URI's userInfo ("user:password" or "user")
    val user = uri.userInfo?.substringBefore(":")
    val password = uri.userInfo?.substringAfter(":", "")?.ifEmpty { null }

    // Parse query string "sslmode=require&connectTimeout=5" into a map of properties
    val urlProperties = mutableMapOf<String, String>()
    uri.query?.split("&")?.forEach { pair ->
        val kv = pair.split("=", limit = 2)
        if (kv.size == 2) {
            urlProperties[kv[0]] = kv[1]
        }
    }

    // Extract schema from extra params
    val schema = extraParams["schema"]

    return PgEndpoint(
        host = host,
        port = port,
        dbName = dbName,
        user = user,
        password = password,
        schema = schema,
        urlProperties = urlProperties,
    )
}

/**
 * Build a [DataSource] from a node config value by parsing the endpoint and
 * applying it to a [PGSimpleDataSource].
 */
private fun createDataSource(nodeValue: String): DataSource {
    val endpoint = parsePgEndpoint(nodeValue)

    val dataSource = PGSimpleDataSource().apply {
        serverNames = arrayOf(endpoint.host)
        portNumbers = intArrayOf(endpoint.port ?: 5432)
        databaseName = endpoint.dbName
        endpoint.user?.let { this.user = it }
        endpoint.password?.let { this.password = it }
        endpoint.schema?.let { this.currentSchema = it }
        endpoint.urlProperties.forEach { (key, value) ->
            setProperty(key, value)
        }
    }

    return HumanReadableDataSource(dataSource, endpoint)
}

private const val NODE_FORMAT = "node = postgresql://user:pwd@host:port/db[,schema=schema-name]  (repeated, one per node)"
