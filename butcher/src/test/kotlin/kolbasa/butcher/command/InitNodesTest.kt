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
 * `init-nodes` with a real config file, on a cluster where no application has started yet
 */
class InitNodesTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testNewCluster() {
        val initialized = run("--yes")

        assertEquals(0, initialized.statusCode, initialized.output)
        listOf("node-01", "node-02", "node-03").forEachIndexed { bucket, node ->
            assertTrue(initialized.output.contains("$node: gets its id, with bucket $bucket"), initialized.output)
            assertTrue(initialized.output.contains("Initialized node $node"), initialized.output)
        }
        assertEquals("node-02", nodeIdOf(dataSourceFirstSchema))

        // Nothing is left to do, so there is nothing to ask about: no --yes and no terminal, and it still succeeds
        val again = run()
        assertEquals(0, again.statusCode, again.output)
        assertTrue(again.output.contains("Every node has its id already"), again.output)
    }

    @Test
    fun testOnlyTheNewNodeGetsAnId() {
        run("--yes")
        // A node that holds no data yet and has lost its id: the troubleshooting advice is to drop q__node
        execute(dataSourceSecondSchema, "drop table q__node")

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("node-03: gets its id, with bucket 2"), result.output)
        assertTrue(result.output.contains("node-01: has its id already"), result.output)
        assertTrue(result.output.contains("node-02: has its id already"), result.output)
        assertTrue(result.output.contains("Initialized node node-03"), result.output)
        assertFalse(result.output.contains("Initialized node node-01"), result.output)
    }

    @Test
    fun testNodeWithMessagesIsWarnedAbout() {
        // A database used without a cluster: a queue with a message, and no q__node. The schema creates one, so it is dropped
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        DatabaseProducer(dataSourceFirstSchema).send(QUEUE, 1).throwExceptionIfAny()
        execute(dataSourceFirstSchema, "drop table q__node")

        val result = run(answer = "n")

        assertTrue(result.output.contains("node-02 has messages but no id yet"), result.output)
        // A warning, not a refusal: the question is still asked
        assertTrue(result.output.contains("3 node(s) will get an id. Continue?"), result.output)
        assertFalse(result.output.contains("node-01 has messages"), result.output)
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
        ButcherCli().subcommands(InitNodes()).test(
            "-c ${config.absolutePath} init-nodes $options",
            stdin = answer.orEmpty(),
            width = 200,
            inputInteractive = answer != null
        )

    private fun execute(dataSource: DataSource, sql: String) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute(sql) }
        }
    }

    private fun nodeIdOf(dataSource: DataSource): String =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("select id from q__node").use { resultSet ->
                    resultSet.next()
                    resultSet.getString(1)
                }
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

    private fun config(publicId: String = "node-01"): File {
        val file = File(tempDir, "cluster.conf")
        file.writeText(
            configLine(publicId, "public") +
                configLine("node-02", FIRST_SCHEMA_NAME) +
                configLine("node-03", SECOND_SCHEMA_NAME)
        )
        return file
    }

    private fun configLine(id: String, schema: String): String =
        "$id  host=${pgContainer.host} port=${pgContainer.firstMappedPort} " +
            "user=${pgContainer.username} password=${pgContainer.password} " +
            "dbname=$databaseName schema=$schema\n"

    private companion object {
        val QUEUE: Queue<Int> = Queue.of("init_nodes_test", PredefinedDataTypes.Int)
    }
}
