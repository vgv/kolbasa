package kolbasa.butcher.command

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.CliktCommandTestResult
import com.github.ajalt.clikt.testing.test
import kolbasa.butcher.ButcherCli
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
 * `sync-shards` with a real config file, on a cluster where no application has started yet
 */
class SyncShardsTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @BeforeEach
    fun before() {
        // The nodes get their ids first: every command but init-nodes refuses a node without one
        val initialized = ButcherCli().subcommands(InitNodes()).test("-c ${config().absolutePath} init-nodes --yes")
        assertEquals(0, initialized.statusCode, initialized.output)
    }

    @Test
    fun testNoShardMap() {
        val result = run("--yes")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("There is no shard map on any node, so there is nothing to sync"), result.output)
        assertTrue(result.output.contains("Initialize the shards first"), result.output)
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            assertFalse(hasTable(node, "q__shard"))
        }
    }

    @Test
    fun testCopyLagsBehind() {
        initShards()
        execute(dataSourceFirstSchema, "delete from q__shard where shard < 10")

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("1 of 3 copies differ"), result.output)
        assertTrue(result.output.contains("node-01: up to date"), result.output)
        assertTrue(result.output.contains("node-02: 10 row(s) changed"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)

        // Nothing is left to do, so there is nothing to ask about: no --yes and no terminal, and it still succeeds
        val again = run()
        assertEquals(0, again.statusCode, again.output)
        assertTrue(again.output.contains("Shard map: all 3 copies are the same"), again.output)
        assertFalse(again.output.contains("row(s) changed"), again.output)
    }

    @Test
    fun testNodeWithoutTable() {
        initShards()
        execute(dataSourceSecondSchema, "drop table q__shard")

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("node-03: 1024 row(s) changed"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)
    }

    @Test
    fun testShardsWithoutRowStayWithoutRow() {
        initShards()
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            execute(node, "delete from q__shard where shard < 12")
        }
        execute(dataSourceFirstSchema, "delete from q__shard where shard = 500")

        val question = run(answer = "n").output
        assertTrue(question.contains("12 shard(s) have no row on any node. This command doesn't give them a node"), question)
        assertTrue(question.contains("1 of 3 copies will be brought up to date"), question)

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("node-02: 1 row(s) changed"), result.output)
        // The copies are the same now, and the shards without a row are left for init-shards
        assertTrue(result.output.contains("Shard map: 12 shard(s) have no row on any node\n"), result.output)
        assertTrue(result.output.contains("No row on any node: 0,1,2,3,4,5,6,7,8,9,10,11"), result.output)
        assertTrue(result.output.contains("use 'init-shards'"), result.output)
    }

    @Test
    fun testNoIsNo() {
        initShards()
        execute(dataSourceFirstSchema, "delete from q__shard where shard < 10")

        val answered = run(answer = "n")
        assertEquals(1, answered.statusCode, answered.output)
        assertTrue(answered.output.contains("nothing was changed"), answered.output)

        // Nothing to read an answer from is not a yes either
        val unattended = run()
        assertEquals(1, unattended.statusCode, unattended.output)
        assertTrue(unattended.output.contains("no terminal"), unattended.output)

        assertEquals(1014, rowsOf(dataSourceFirstSchema))
    }

    @Test
    fun testYesIsYes() {
        initShards()
        execute(dataSourceFirstSchema, "delete from q__shard where shard < 10")

        val answered = run(answer = "y")

        assertEquals(0, answered.statusCode, answered.output)
        assertTrue(answered.output.contains("Shard map: all 3 copies are the same"), answered.output)
    }

    // ---------- helpers ----------

    private fun initShards() {
        val result = ButcherCli().subcommands(InitShards()).test("-c ${config().absolutePath} init-shards --yes")
        assertEquals(0, result.statusCode, result.output)
    }

    private fun run(options: String = "", answer: String? = null): CliktCommandTestResult =
        ButcherCli().subcommands(SyncShards()).test(
            "-c ${config().absolutePath} sync-shards $options",
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
}
