package kolbasa.cluster.admin

import kolbasa.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.postgresql.ds.PGSimpleDataSource

class ClusterAdminTest : AbstractPostgreSQLTest() {

    @Test
    fun testEmptyNodeListIsRejected() {
        assertThrows<IllegalArgumentException> {
            ClusterAdmin(emptyList())
        }
    }

    @Test
    fun testCreatingAdminChangesNothingInTheDatabase() {
        val tablesBefore = publicTables()

        ClusterAdmin(listOf(dataSource))

        // An operator reaches for ClusterAdmin when the cluster is already broken, so it must not create or
        // seed anything on its way in - unlike Cluster, which initializes what it needs when it starts
        assertEquals(tablesBefore, publicTables())
    }

    @Test
    fun testCreatingAdminDoesNotConnect() {
        val unreachable = PGSimpleDataSource().apply {
            serverNames = arrayOf("127.0.0.1")
            portNumbers = intArrayOf(9)
            databaseName = "there-is-no-such-database"
        }

        // Constructing is fine, an unreachable node only becomes a problem when the cluster is asked
        val admin = ClusterAdmin(listOf(unreachable))

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            admin.checkNodesAvailable()
        }
        assertEquals(true, exception.message.contains("Can't connect to"), exception.message)
    }

    private fun publicTables(): Int {
        return dataSource.readInt("select count(*) from information_schema.tables where table_schema='public'")
    }

}
