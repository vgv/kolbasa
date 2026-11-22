package kolbasa.butcher.command

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.test
import kolbasa.butcher.ButcherCli
import kolbasa.cluster.Cluster
import kolbasa.test.AbstractPostgreSQLTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import javax.sql.DataSource

/**
 * `check-cluster` with a real config file, on a cluster where no application has started yet and on one where it has
 */
class CheckClusterTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testNewCluster_NodesGetTheirIdsAndNothingElse() {
        val result = ButcherCli().subcommands(CheckCluster()).test("-c ${config().absolutePath} check-cluster")

        // The checks ran, and two of them found something: no shard map yet
        assertEquals(2, result.statusCode, result.output)
        assertTrue(result.output.contains("Initialized node node-01"), result.output)
        assertTrue(result.output.contains("no shard belongs to a node of this cluster"), result.output)
        assertTrue(result.output.contains("no shard map on any node yet"), result.output)
        assertTrue(result.output.contains("'create-or-repair-shard-map'"), result.output)
        assertTrue(result.output.contains("2 of 5 check(s) found something: shard-balance, shard-map."), result.output)
        assertFalse(result.output.contains("Completed successfully"), result.output)

        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            assertTrue(hasTable(node, "q__node"))
            assertFalse(hasTable(node, "q__shard"))
        }
    }

    @Test
    fun testShardMapCheckAlone() {
        Cluster(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema), emptyList()).updateTopology()
        val (first, second, third) = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).map { nodeIdOf(it) }

        val result = ButcherCli().subcommands(CheckCluster())
            .test("-c ${config(first, second, third).absolutePath} check-cluster --checks=shard-map")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Shard map: all 3 copies are the same"), result.output)
        assertFalse(result.output.contains("Shard balance"), result.output)
        assertTrue(result.output.trimEnd().endsWith("All checks passed."), result.output)
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

    private fun config(publicId: String = "node-01", firstId: String = "node-02", secondId: String = "node-03"): File {
        val file = File(tempDir, "cluster.conf")
        file.writeText(
            configLine(publicId, "public") +
                configLine(firstId, FIRST_SCHEMA_NAME) +
                configLine(secondId, SECOND_SCHEMA_NAME)
        )
        return file
    }

    private fun configLine(id: String, schema: String): String =
        "$id  host=${pgContainer.host} port=${pgContainer.firstMappedPort} " +
            "user=${pgContainer.username} password=${pgContainer.password} " +
            "dbname=$databaseName schema=$schema\n"
}
