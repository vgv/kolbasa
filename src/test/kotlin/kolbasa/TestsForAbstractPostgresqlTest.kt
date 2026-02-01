package kolbasa

import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.readString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test


// Yes, this is a test class for test class AbstractPostgresqlTest :)
internal class TestsForAbstractPostgresqlTest : AbstractPostgresqlTest() {

    @Test
    fun checkSchemas() {
        // Create a few tables with the same name in different schemas and put one row into each table
        // When, try to check that all these tables were really created in different schemas

        assertEquals(1, dataSource.readInt("select count(*) from test"))
        assertEquals("public", dataSource.readString("select str from test"))

        assertEquals(1, dataSourceFirstSchema.readInt("select count(*) from test"))
        assertEquals("first", dataSourceFirstSchema.readString("select str from test"))
        // and make a "dirty" read just using an explicit schema name
        assertEquals("first", dataSource.readString("select str from ${FIRST_SCHEMA_NAME}.test"))

        assertEquals(1, dataSourceSecondSchema.readInt("select count(*) from test"))
        assertEquals("second", dataSourceSecondSchema.readString("select str from test"))
        // and make a "dirty" read just using an explicit schema name
        assertEquals("second", dataSource.readString("select str from ${SECOND_SCHEMA_NAME}.test"))
    }

    override fun generateTestData(): List<String> {
        return listOf(
            "create table test(str varchar)",
            "insert into test(str) values('public')"
        )
    }

    override fun generateTestDataFirstSchema(): List<String> {
        return listOf(
            "create table test(str varchar)",
            "insert into test(str) values('first')"
        )
    }

    override fun generateTestDataSecondSchema(): List<String> {
        return listOf(
            "create table test(str varchar)",
            "insert into test(str) values('second')"
        )
    }
}
