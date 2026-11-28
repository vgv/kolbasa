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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import javax.sql.DataSource

/**
 * `init-shards` with a real config file, on a cluster where no application has started yet
 */
class InitShardsTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @BeforeEach
    fun before() {
        // The nodes get their ids first: every command but init-nodes refuses a node without one
        val initialized = ButcherCli().subcommands(InitNodes()).test("-c ${config().absolutePath} init-nodes --yes")
        assertEquals(0, initialized.statusCode, initialized.output)
    }

    @Test
    fun testNewCluster() {
        val created = run("--yes")

        assertEquals(0, created.statusCode, created.output)
        assertTrue(created.output.contains("node-01: gets 342 shard(s)"), created.output)
        assertTrue(created.output.contains("node-02: gets 341 shard(s)"), created.output)
        assertTrue(created.output.contains("node-03: gets 341 shard(s)"), created.output)
        assertTrue(created.output.contains("node-01: got 342 shard(s)"), created.output)
        assertTrue(created.output.contains("Shard map: all 3 copies are the same"), created.output)

        // Nothing is left to do, so there is nothing to ask about: no --yes and no terminal, and it still succeeds
        val again = run()
        assertEquals(0, again.statusCode, again.output)
        assertTrue(again.output.contains("Every shard has a node already"), again.output)
        assertTrue(again.output.contains("Shard map: all 3 copies are the same"), again.output)
    }

    @Test
    fun testOnlyShardsWithoutRow() {
        run("--yes")
        // Shard 5 was node-03's, shard 6 node-01's
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            execute(node, "delete from q__shard where shard in (5, 6)")
        }

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        // node-03 has the fewest shards, then the smallest id wins the tie
        assertTrue(result.output.contains("node-01: gets 1 shard(s)"), result.output)
        assertTrue(result.output.contains("node-03: gets 1 shard(s)"), result.output)
        assertFalse(result.output.contains("node-02: gets"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)
    }

    @Test
    fun testCopiesThatDifferAreForSyncShards() {
        run("--yes")
        execute(dataSourceFirstSchema, "delete from q__shard where shard < 10")

        // Every shard has a row on some node, so there is nothing to ask about
        val result = run()

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Every shard has a node already"), result.output)
        assertTrue(result.output.contains("1 of 3 copies differ"), result.output)
        assertTrue(result.output.contains("'sync-shards'"), result.output)
        assertEquals(1014, rowsOf(dataSourceFirstSchema))
    }

    @Test
    fun testNodeWithMessagesIsWarnedAbout() {
        // A database used without a cluster, which has its id already: a queue with a message
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        DatabaseProducer(dataSourceFirstSchema).send(QUEUE, 1).throwExceptionIfAny()

        val result = run(answer = "n")

        assertTrue(result.output.contains("node-02 has messages, and the cluster has no shard map yet"), result.output)
        // A warning, not a refusal: the question is still asked
        assertTrue(result.output.contains("1024 shard(s) will get a node. Continue?"), result.output)
        assertFalse(result.output.contains("node-01 has messages"), result.output)

        // The node that gets every shard keeps its messages readable
        val target = run("--target=node-02", answer = "n")
        assertTrue(target.output.contains("node-02: gets 1024 shard(s)"), target.output)
        assertFalse(target.output.contains("has messages"), target.output)
    }

    @Test
    fun testTarget() {
        val result = run("--target=node-02 --yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("node-02: gets 1024 shard(s)"), result.output)
        assertTrue(result.output.contains("node-02: got 1024 shard(s)"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)
    }

    @Test
    fun testTargetAndShards() {
        run("--yes")
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            execute(node, "delete from q__shard where shard in (5, 6, 7)")
        }

        val result = run("--target=node-03 --shards=5,6 --yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("node-03: gets 2 shard(s)"), result.output)
        assertTrue(result.output.contains("node-03: got 2 shard(s)"), result.output)
        // Shard 7 wasn't listed
        assertTrue(result.output.contains("No row on any node: 7"), result.output)
    }

    @Test
    fun testShardsNeedTarget() {
        val result = run("--shards=5 --yes")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--shards needs --target"), result.output)
        // The usage of this command, not of butcher
        assertTrue(result.output.contains("init-shards [<options>]"), result.output)
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            assertFalse(hasTable(node, "q__shard"))
        }
    }

    @Test
    fun testShardOutOfRange() {
        val result = run("--target=node-03 --shards=5,5000 --yes")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("shard(s) out of range [0..1023]: 5000"), result.output)
    }

    @Test
    fun testShardWithRowIsRefusedBeforeTheQuestion() {
        run("--yes")
        execute(dataSource, "delete from q__shard where shard = 5")

        // No --yes and no terminal: a refusal after the question would say "no terminal" instead
        val result = run("--target=node-03 --shards=5")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Shard(s) 5 have a node already"), result.output)
        assertTrue(result.output.contains("to change the node of a shard, migrate it"), result.output)
    }

    @Test
    fun testUnknownTarget() {
        val result = run("--target=node-99")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Target node node-99 is not a member of this cluster"), result.output)
    }

    @Test
    fun testAnotherNodeWonTheRace() {
        // The nodes get their tables, with shard 0
        run("--target=node-02 --shards=0 --yes")
        // An application gives shard 5 to node-01 at the same moment: its row is on the node with the smallest id first.
        // Every new row of shard 5 on that node turns into that one
        execute(
            dataSource,
            "create function application_row() returns trigger language plpgsql as " +
                "'begin if new.shard = 5 then new.producer_node = ''node-01''; new.consumer_node = ''node-01''; end if; " +
                "return new; end'"
        )
        execute(
            dataSource,
            "create trigger application_row before insert on q__shard for each row execute procedure application_row()"
        )

        val result = run("--target=node-03 --shards=5,6 --yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("node-01: got 1 shard(s)"), result.output)
        assertTrue(result.output.contains("node-03: got 1 shard(s)"), result.output)
        val text = result.output.replace(Regex("\\s+"), " ")
        assertTrue(text.contains("Shard(s) 5 went to node-01: an application gave them a node at the same moment"), text)
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
            assertFalse(hasTable(node, "q__shard"))
        }
    }

    @Test
    fun testYesIsYes() {
        val answered = run(answer = "y")

        assertEquals(0, answered.statusCode, answered.output)
        assertTrue(answered.output.contains("Shard map: all 3 copies are the same"), answered.output)
    }

    // ---------- helpers ----------

    private fun run(options: String = "", answer: String? = null): CliktCommandTestResult =
        ButcherCli().subcommands(InitShards()).test(
            "-c ${config().absolutePath} init-shards $options",
            stdin = answer.orEmpty(),
            width = 200,
            inputInteractive = answer != null
        )

    private fun execute(dataSource: DataSource, sql: String) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute(sql) }
        }
    }

    private fun rowsOf(dataSource: DataSource): Int =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("select count(*) from q__shard").use { resultSet ->
                    resultSet.next()
                    resultSet.getInt(1)
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

    private fun config(): File {
        val file = File(tempDir, "cluster.conf")
        file.writeText(
            configLine("node-01", "public") +
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
        val QUEUE: Queue<Int> = Queue.of("init_shards_test", PredefinedDataTypes.Int)
    }
}
