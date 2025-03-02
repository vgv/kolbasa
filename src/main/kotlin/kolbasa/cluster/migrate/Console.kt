package kolbasa.cluster.migrate

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import kolbasa.cluster.migrate.utils.HumanReadableDataSource
import kolbasa.pg.DatabaseExtensions.readString
import org.postgresql.ds.PGSimpleDataSource
import javax.sql.DataSource
import kotlin.system.exitProcess

internal class PrepareCommand {
    @Parameter(names = ["-target"], required = true, description = "Shard to prepare")
    lateinit var target: String

    @Parameter(
        names = ["-shard", "-shards"],
        required = true,
        description = "Shard option can be used multiple times, and may be comma-separated"
    )
    lateinit var shards: List<Int>

    @Parameter(names = ["-cluster"], required = true, description = "Cluster")
    lateinit var cluster: String
}

internal class MoveCommand {
    @Parameter(names = ["-cluster"], required = true, description = "Cluster")
    lateinit var cluster: String

    @Parameter(names = ["-tables"], required = false, description = "Tables")
    var tables: List<String> = emptyList()
}

fun main(args: Array<String>) {
    val prepareCommand = PrepareCommand()
    val moveCommand = MoveCommand()

    val jc = JCommander.newBuilder()
        .addCommand("prepare", prepareCommand)
        .addCommand("move", moveCommand)
        .build()

    if (args.isEmpty()) {
        jc.usage()
        return
    }

    jc.parse(*args)
    when (jc.parsedCommand) {
        "prepare" -> launchPrepare(prepareCommand)
        "move" -> launchMove(moveCommand)
    }
}

private fun launchPrepare(command: PrepareCommand) {
    val dataSources = convertClusterStringToDataSource(command.cluster)
    checkDataSources(dataSources)

    prepare(command.shards, command.target, dataSources, ConsoleMigrateEvents())
}

private fun launchMove(command: MoveCommand) {
    val dataSources = convertClusterStringToDataSource(command.cluster)
    checkDataSources(dataSources)

    val tablesToFind = if (command.tables.isEmpty()) {
        null
    } else {
        command.tables.toSet()
    }

    migrate(tablesToFind, dataSources, ConsoleMigrateEvents())
}


private fun convertClusterStringToDataSource(cluster: String): List<DataSource> {
    val dataSources = mutableListOf<DataSource>()

    for (node in cluster.split(";")) {
        val urlParts = node.split(",", limit = 3)
        if (urlParts.size < 2) {
            println("Invalid node format: $node. Correct format: url1,user1,password1;url2,user2,password2... (password is optional)")
            exitProcess(1)
        }

        val url = urlParts[0]
        val usr = urlParts[1]
        val pwd = urlParts.getOrNull(2)

        val dataSource = PGSimpleDataSource().apply {
            setURL(url)
            user = usr
            password = pwd
        }
        dataSources += HumanReadableDataSource(dataSource, url)
    }

    return dataSources
}

private fun checkDataSources(dataSources: List<DataSource>) {
    var errorsFound = false

    dataSources.forEach { dataSource ->
        try {
            dataSource.readString("select 1")
        } catch (e: Exception) {
            errorsFound = true

            var message = "Can't connect to $dataSource: ${e.message}"
            e.cause?.let {
                message += ", ${it.message}"
            }
            println(message)
        }
    }

    if (errorsFound) {
        exitProcess(1)
    }
}
