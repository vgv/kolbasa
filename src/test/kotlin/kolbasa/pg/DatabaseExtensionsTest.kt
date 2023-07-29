package kolbasa.pg

import kolbasa.AbstractPostgresTest
import kolbasa.pg.DatabaseExtensions.readBoolean
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.readIntList
import kolbasa.pg.DatabaseExtensions.readLong
import kolbasa.pg.DatabaseExtensions.readLongList
import kolbasa.pg.DatabaseExtensions.readStringList
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.pg.DatabaseExtensions.useSavepoint
import kolbasa.pg.DatabaseExtensions.useStatement
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.*

internal class DatabaseExtensionsTest : AbstractPostgresTest() {

    override fun generateTestData(): List<String> {
        val statements = mutableListOf<String>()
        // empty_table
        statements += "create table empty_table(str_value varchar(200), int_value int, long_value bigint)"
        // full_table
        statements += "create table full_table(str_value varchar(200), int_value int, long_value bigint, boolean_value boolean)"
        statements += "insert into full_table(str_value,int_value,long_value,boolean_value) values ('a',1,10,false),('b',2,20,true),('c',3,30,false)"

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
        var firstTransaction: Long = -1
        var secondTransaction: Long = -1

        dataSource.useConnection { connection: Connection ->
            assertEquals(3, connection.readInt("select count(*) from full_table"))
            connection.useStatement { statement -> statement.executeUpdate("delete from full_table") }
            assertEquals(0, connection.readInt("select count(*) from full_table"))
            firstTransaction = connection.readLong("select txid_current()")

            // read in another transaction
            dataSource.useConnection { otherConnection ->
                assertEquals(3, otherConnection.readInt("select count(*) from full_table"))
                secondTransaction = otherConnection.readLong("select txid_current()")
            }

            // read again in the first transaction
            assertEquals(0, connection.readInt("select count(*) from full_table"))
        }

        // read again after commit^ above
        dataSource.useConnection { connection: Connection ->
            assertEquals(0, connection.readInt("select count(*) from full_table"))
        }

        // Check that
        assertNotEquals(-1L, firstTransaction) // transaction id was assigned
        assertNotEquals(-1L, secondTransaction) // transaction id was assigned
        assertNotEquals(firstTransaction, secondTransaction) // transactions were really different
    }

    // -------------------------------------------------------------------------------------------

    @Test
    fun testUseSavepoint() {
        dataSource.useConnection { dataSourceConnection ->
            assertEquals(3, dataSourceConnection.readInt("select count(*) from full_table"))

            // Successful savepoint
            dataSourceConnection.useSavepoint { savepointConnection ->
                // Connection in a savepoint block must be the same
                assertSame(dataSourceConnection, savepointConnection)
                savepointConnection.useStatement { statement ->
                    statement.executeUpdate("insert into full_table(str_value,int_value,long_value,boolean_value) values ('d',4,40,true)")
                }
            }

            // Invalid savepoint
            dataSourceConnection.useSavepoint { savepointConnection ->
                // Connection in a savepoint block must be the same
                assertSame(dataSourceConnection, savepointConnection)
                savepointConnection.useStatement { statement ->
                    // Insert into table with wrong name, PG throws an exception
                    statement.executeUpdate("insert into full_table_wrong_name(str_value,int_value,long_value,boolean_value) values ('d',4,40,true)")
                }
            }

            // Successful savepoint
            dataSourceConnection.useSavepoint { savepointConnection ->
                // Connection in a savepoint block must be the same
                assertSame(dataSourceConnection, savepointConnection)
                savepointConnection.useStatement { statement ->
                    statement.executeUpdate("insert into full_table(str_value,int_value,long_value,boolean_value) values ('e',5,50,true)")
                }
            }

            // Check inside the same transaction
            assertEquals(5, dataSourceConnection.readInt("select count(*) from full_table"))
        }

        // Check outside of transaction
        assertEquals(5, dataSource.readInt("select count(*) from full_table"))
        assertEquals(listOf(1, 2, 3, 4, 5), dataSource.readIntList("select int_value from full_table order by int_value"))
    }

    // -------------------------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------------------------

    @Test
    fun testReadInt() {
        val value = dataSource.readInt("select int_value from full_table where str_value='a'")
        assertEquals(1, value)
    }

    @Test
    fun testReadInt_NoRows() {
        assertFailsWith<IllegalArgumentException> {
            // No rows with str_value == 'z'
            dataSource.readInt("select int_value from full_table where str_value='z'")
        }
    }

    @Test
    fun testReadInt_MoreThanOneRow() {
        assertFailsWith<IllegalArgumentException> {
            // No rows with str_value == 'z'
            dataSource.readInt("select int_value from full_table")
        }
    }
    // -------------------------------------------------------------------------------------------

    @Test
    fun testReadLong() {
        val value = dataSource.readLong("select long_value from full_table where str_value='a'")
        assertEquals(10, value)
    }

    @Test
    fun testReadLong_NoRows() {
        assertFailsWith<IllegalArgumentException> {
            // No rows with str_value == 'z'
            dataSource.readLong("select long_value from full_table where str_value='z'")
        }
    }

    @Test
    fun testReadLong_MoreThanOneRow() {
        assertFailsWith<IllegalArgumentException> {
            // No rows with str_value == 'z'
            dataSource.readLong("select long_value from full_table")
        }
    }

    // -------------------------------------------------------------------------------------------

    @Test
    fun testReadBoolean() {
        val value = dataSource.readBoolean("select boolean_value from full_table where str_value='b'")
        assertTrue(value)
    }

    @Test
    fun testReadBoolean_NoRows() {
        assertFailsWith<IllegalArgumentException> {
            // No rows with str_value == 'z'
            dataSource.readBoolean("select boolean_value from full_table where str_value='z'")
        }
    }

    @Test
    fun testReadBoolean_MoreThanOneRow() {
        assertFailsWith<IllegalArgumentException> {
            // No rows with str_value == 'z'
            dataSource.readBoolean("select boolean_value from full_table")
        }
    }

}
