package kolbasa.butcher.command

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.CliktCommandTestResult
import com.github.ajalt.clikt.testing.test
import kolbasa.butcher.ButcherCli
import kolbasa.test.AbstractPostgreSQLTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import javax.sql.DataSource

/**
 * `create-or-repair-shard-map` with a real config file, on a cluster where no application has started yet
 */
class CreateOrRepairShardMapTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testNewCluster() {
        val created = run("--yes")

        assertEquals(0, created.statusCode, created.output)
        assertTrue(created.output.contains("no shard map on any node yet"), created.output)
        listOf("node-01", "node-02", "node-03").forEach { node ->
            assertTrue(created.output.contains("$node: 1024 row(s) changed"), created.output)
        }
        assertTrue(created.output.contains("Shard map: all 3 copies are the same"), created.output)

        // Nothing is left to do, so there is nothing to ask about: no --yes and no terminal, and it still succeeds
        val again = run()
        assertEquals(0, again.statusCode, again.output)
        assertTrue(again.output.contains("Shard map: all 3 copies are the same"), again.output)
        assertFalse(again.output.contains("row(s) changed"), again.output)
    }

    @Test
    fun testCopyLagsBehind() {
        run("--yes")
        execute(dataSourceFirstSchema, "delete from q__shard where shard < 10")

        val result = run("--yes")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("1 of 3 copies differ"), result.output)
        assertTrue(result.output.contains("node-01: up to date"), result.output)
        assertTrue(result.output.contains("node-02: 10 row(s) changed"), result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)
    }

    @Test
    fun testQuestionSaysWhatWillHappen() {
        val newCluster = run(answer = "n")
        assertTrue(newCluster.output.contains("A new one will be created on 3 node(s)"), newCluster.output)

        run("--yes")
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            execute(node, "delete from q__shard where shard < 12")
        }
        execute(dataSourceFirstSchema, "delete from q__shard where shard = 500")

        val damaged = run(answer = "n")
        assertTrue(damaged.output.contains("12 shard(s) have no row on any node"), damaged.output)
        assertTrue(damaged.output.contains("1 of 3 copies will be brought up to date"), damaged.output)
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
        ButcherCli().subcommands(CreateOrRepairShardMap()).test(
            "-c ${config().absolutePath} create-or-repair-shard-map $options",
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
