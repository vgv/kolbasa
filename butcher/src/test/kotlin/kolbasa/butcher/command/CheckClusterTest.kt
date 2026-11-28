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
    fun testNewCluster_RefusedAndNothingWritten() {
        val result = ButcherCli().subcommands(CheckCluster()).test("-c ${config().absolutePath} check-cluster")

        // check-cluster gives no ids, it says that the nodes need them first
        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Node(s) node-01, node-02, node-03 have no id yet"), result.output)
        assertTrue(result.output.contains("Initialize the cluster first"), result.output)

        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { node ->
            assertFalse(hasTable(node, "q__node"))
            assertFalse(hasTable(node, "q__shard"))
        }
    }

    @Test
    fun testNewCluster_AfterInitNodes() {
        val initialized = ButcherCli().subcommands(InitNodes()).test("-c ${config().absolutePath} init-nodes --yes")
        assertEquals(0, initialized.statusCode, initialized.output)

        // Without a shard map the cluster is not set up yet, whichever checks are asked for: they can't run
        listOf("", "--checks=schema-consistency").forEach { options ->
            val result = ButcherCli().subcommands(CheckCluster()).test("-c ${config().absolutePath} check-cluster $options")

            assertEquals(1, result.statusCode, result.output)
            assertTrue(result.output.contains("There is no shard map on any node. Initialize the cluster first"), result.output)
            assertFalse(result.output.contains("found something"), result.output)
        }

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
