package kolbasa.cluster.admin

import kolbasa.test.AbstractPostgreSQLTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.postgresql.ds.PGSimpleDataSource

class ClusterAdminHelpersTest : AbstractPostgreSQLTest() {

    @Test
    fun testCheckNodesAvailable_All_Reachable() {
        ClusterAdminHelpers.checkNodesAvailable(listOf(dataSource, dataSourceFirstSchema))
    }

    @Test
    fun testCheckNodesAvailable_Unreachable_Node() {
        val unreachable = PGSimpleDataSource().apply {
            serverNames = arrayOf("127.0.0.1")
            portNumbers = intArrayOf(9)
            databaseName = "there-is-no-such-database"
        }

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            ClusterAdminHelpers.checkNodesAvailable(listOf(dataSource, unreachable))
        }

        assertTrue(exception.message.contains("Can't connect to 1 of 2"), exception.message)
    }
}
