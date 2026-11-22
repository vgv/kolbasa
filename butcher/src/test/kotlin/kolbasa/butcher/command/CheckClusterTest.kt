package kolbasa.butcher.command

import com.github.ajalt.clikt.core.subcommands
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
 * `check-cluster` on a cluster where no application has started yet
 */
class CheckClusterTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testNewCluster_NodesGetTheirIdsAndNothingElse() {
        val result = ButcherCli().subcommands(CheckCluster()).test("check-cluster ${config().absolutePath}")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Initialized node node-01"), result.output)
        assertTrue(result.output.contains("no shard belongs to a node of this cluster"), result.output)

        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            assertTrue(hasTable(node, "q__node"))
            assertFalse(hasTable(node, "q__shard"))
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
