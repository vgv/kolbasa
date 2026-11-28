package kolbasa.butcher.command

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.CliktCommandTestResult
import com.github.ajalt.clikt.testing.test
import kolbasa.butcher.ButcherCli
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import javax.sql.DataSource

/**
 * `init-cluster` with a real config file, on a cluster where no application has started yet
 */
class InitClusterTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testNewCluster() {
        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        listOf("node-01", "node-02", "node-03").forEachIndexed { bucket, node ->
            assertTrue(result.output.contains("$node: gets its id, with bucket $bucket"), result.output)
            assertTrue(result.output.contains("Initialized node $node"), result.output)
        }
        assertTrue(result.output.contains("node-01: gets 342 shard(s)"), result.output)
        assertTrue(result.output.contains("node-01: got 342 shard(s)"), result.output)
        assertTrue(result.output.contains("node-03: got 341 shard(s)"), result.output)
        // The new rows went to every copy, so there was nothing to sync
        assertFalse(result.output.contains("row(s) of the shard map changed"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)

        // The cluster is set up: every check passes
        val checked = ButcherCli().subcommands(CheckCluster()).test("-c ${config().absolutePath} check-cluster")
        assertEquals(0, checked.statusCode, checked.output)

        // Nothing is left to do, so there is nothing to ask about: no --yes and no terminal, and it still succeeds
        val again = run()
        assertEquals(0, again.statusCode, again.output)
        assertTrue(again.output.contains("The cluster is initialized already"), again.output)
        assertTrue(again.output.contains("Shard map: all 3 copies are the same"), again.output)
    }

    @Test
    fun testNewNodeJoins() {
        val twoNodes = run("--yes", config = config(nodes = 2))
        assertEquals(0, twoNodes.statusCode, twoNodes.output)

        val question = run(answer = "n")
        assertTrue(question.output.contains("node-03: gets its id, with bucket 2"), question.output)
        assertFalse(question.output.contains("gets its id, with bucket 0"), question.output)
        assertTrue(question.output.contains("1 of 3 copies of the shard map will be brought up to date"), question.output)
        assertTrue(question.output.contains("1 node(s) will get an id, 1 of 3 copies"), question.output)
        assertFalse(question.output.contains("shard(s) will get a node"), question.output)

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Initialized node node-03"), result.output)
        assertTrue(result.output.contains("node-03: 1024 row(s) of the shard map changed"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)
    }

    @Test
    fun testNodeWithMessagesIsWarnedAbout() {
        // A database used without a cluster: a queue with a message, and no q__node. The schema creates one, so it is dropped
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        DatabaseProducer(dataSourceFirstSchema).send(QUEUE, 1).throwExceptionIfAny()
        execute(dataSourceFirstSchema, "drop table q__node")

        val result = run(answer = "n")
        val text = result.output.replace(Regex("\\s+"), " ")

        assertTrue(text.contains("node-02 has messages, and the cluster has no shard map yet"), result.output)
        assertTrue(text.contains("Its message ids can collide with another node's as well"), result.output)
        assertTrue(text.contains("Initialize the cluster with this node alone first, then add the other nodes"), result.output)
        // A warning, not a refusal: the question is still asked
        assertTrue(text.contains("3 node(s) will get an id, 1024 shard(s) will get a node. Continue?"), result.output)
        assertFalse(text.contains("node-01 has messages"), result.output)
    }

    @Test
    fun testNoIsNo() {
        val answered = run(answer = "n")
        assertEquals(1, answered.statusCode, answered.output)
        assertTrue(answered.output.contains("nothing was changed"), answered.output)

        // Nothing to read an answer from is not a yes either
        val unattended = run()
        assertEquals(1, unattended.statusCode, unattended.output)
        assertTrue(unattended.output.contains("no terminal"), unattended.output)

        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            assertFalse(hasTable(node, "q__node"))
            assertFalse(hasTable(node, "q__shard"))
        }
    }

    @Test
    fun testWrongIdIsRefusedBeforeTheQuestion() {
        run("--yes")
        execute(dataSourceSecondSchema, "drop table q__node")

        // No --yes and no terminal: a refusal after the question would say "no terminal" instead
        val result = run(config = config(publicId = "node-99"))

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Node ids don't match"), result.output)
        // The list is wrong, so the new node is not set up either
        assertFalse(hasTable(dataSourceSecondSchema, "q__node"))
    }

    // ---------- helpers ----------

    private fun run(options: String = "", answer: String? = null, config: File = config()): CliktCommandTestResult =
        ButcherCli().subcommands(InitCluster()).test(
            "-c ${config.absolutePath} init-cluster $options",
            stdin = answer.orEmpty(),
            width = 200,
            inputInteractive = answer != null
        )

    private fun execute(dataSource: DataSource, sql: String) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute(sql) }
        }
    }

    private fun hasTable(dataSource: DataSource, table: String): Boolean =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("select to_regclass('$table') is not null").use { resultSet ->
                    resultSet.next() && resultSet.getBoolean(1)
                }
            }
        }

    // The first [nodes] of node-01, node-02 and node-03, the first one under [publicId]
    private fun config(nodes: Int = 3, publicId: String = "node-01"): File {
        val lines = listOf(
            configLine(publicId, "public"),
            configLine("node-02", FIRST_SCHEMA_NAME),
            configLine("node-03", SECOND_SCHEMA_NAME)
        )
        val file = File(tempDir, "cluster-$nodes-$publicId.conf")
        file.writeText(lines.take(nodes).joinToString(""))
        return file
    }

    private fun configLine(id: String, schema: String): String =
        "$id  host=${pgContainer.host} port=${pgContainer.firstMappedPort} " +
            "user=${pgContainer.username} password=${pgContainer.password} " +
            "dbname=$databaseName schema=$schema\n"

    private companion object {
        val QUEUE: Queue<Int> = Queue.of("init_cluster_test", PredefinedDataTypes.Int)
    }
}
