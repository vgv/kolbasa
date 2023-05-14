package kolbasa.pg

import kolbasa.AbstractPostgresTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.readIntList
import kolbasa.pg.DatabaseExtensions.readLongList
import kolbasa.pg.DatabaseExtensions.readStringList
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.pg.DatabaseExtensions.useStatement
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.Connection

internal class DatabaseExtensionsTest : AbstractPostgresTest() {

    override fun generateTestData(): List<String> {
        val statements = mutableListOf<String>()
        statements += "create table empty_table(str_value varchar(200), int_value int, long_value bigint)"
        statements += "create table full_table(str_value varchar(200), int_value int, long_value bigint)"
        statements += "insert into full_table(str_value,int_value,long_value) values ('a',1,10),('b',2,20),('c',3,30)"

        return statements
    }

    @Test
    fun testUseConnection_CheckAutoCommit() {
        // Check auto-commit is off
        dataSource.useConnection { connection: Connection ->
            assertFalse(connection.autoCommit)
        }
    }

    @Test
    fun testUseConnection_CheckTransactionBoundaries() {
        dataSource.useConnection { connection: Connection ->
            assertEquals(3, connection.readInt("select count(*) from full_table"))
            connection.useStatement { statement -> statement.executeUpdate("delete from full_table") }
            assertEquals(0, connection.readInt("select count(*) from full_table"))

            // read in another transaction
            dataSource.useConnection { otherConnection ->
                assertEquals(3, otherConnection.readInt("select count(*) from full_table"))
            }

            // read again in the first transaction
            assertEquals(0, connection.readInt("select count(*) from full_table"))
        }

        // read again after commit^ above
        dataSource.useConnection { connection: Connection ->
            assertEquals(0, connection.readInt("select count(*) from full_table"))
        }
    }

    @Test
    fun testReadStringList_ifEmpty() {
        val list = dataSource.readStringList("select str_value from empty_table")
        assertTrue(list.isEmpty())
    }

    @Test
    fun testReadStringList() {
        val list = dataSource.readStringList("select str_value from full_table order by str_value")
        assertEquals(listOf("a", "b", "c"), list)
    }

    @Test
    fun testReadIntList_ifEmpty() {
        val list = dataSource.readIntList("select int_value from empty_table")
        assertTrue(list.isEmpty())
    }

    @Test
    fun testReadIntList() {
        val list = dataSource.readIntList("select int_value from full_table order by int_value")
        assertEquals(listOf(1, 2, 3), list)
    }

    @Test
    fun testReadLongList_ifEmpty() {
        val list = dataSource.readLongList("select long_value from empty_table")
        assertTrue(list.isEmpty())
    }

    @Test
    fun testReadLongList() {
        val list = dataSource.readLongList("select long_value from full_table order by long_value")
        assertEquals(listOf<Long>(10, 20, 30), list)
    }
}
