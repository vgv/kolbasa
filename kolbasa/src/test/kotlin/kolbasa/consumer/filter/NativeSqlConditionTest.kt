package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.PreparedStatement

class NativeSqlConditionTest {

    private val intField = MetaField.ofInt("int_value", FieldOption.SEARCH)
    private val stringField = MetaField.ofString("string_value", FieldOption.SEARCH)

    @Test
    fun testToSql() {
        val nativeExpression = NativeSqlCondition(
            sqlPattern = "({0} like 'a%') or ({0} like 'b%') and {1} is null and {2} is null",
            fields = listOf(stringField, intField, stringField)
        )

        val sql = nativeExpression.toSqlClause()
        assertEquals(
            "(meta_string_value like 'a%') or (meta_string_value like 'b%') and meta_int_value is null and meta_string_value is null",
            sql
        )
    }

    /**
     * Only `{N}` placeholders are replaced. A quote doubled inside a literal and braces that aren't a placeholder go
     * into SQL as written.
     */
    @Test
    fun testToSql_KeepsTheRestAsWritten() {
        val nativeExpression = NativeSqlCondition(
            sqlPattern = "{0} = 'it''s' and {1} = any('{1,2}'::int[]) and {0} <> '{x}'",
            fields = listOf(stringField, intField)
        )

        val sql = nativeExpression.toSqlClause()
        assertEquals(
            "meta_string_value = 'it''s' and meta_int_value = any('{1,2}'::int[]) and meta_string_value <> '{x}'",
            sql
        )
    }

    /**
     * A placeholder without a field fails at once, when the condition is created, not later in PostgreSQL.
     */
    @Test
    fun testToSql_PlaceholderWithoutField() {
        val exception = assertThrows<IllegalArgumentException> {
            NativeSqlCondition(sqlPattern = "{0} = {2}", fields = listOf(stringField, intField))
        }
        assertEquals("Pattern '{0} = {2}' has {2}, but there are only 2 fields", exception.message)
    }

    @Test
    fun testFillPreparedQuery() {
        val nativeExpression = NativeSqlCondition(
            sqlPattern = "{0} is null",
            fields = listOf(stringField)
        )
        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        nativeExpression.toSqlClause()
        nativeExpression.fillPreparedQuery(preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    @Test
    fun testEqualsAndHashCode() {
        val condition1 = NativeSqlCondition(
            sqlPattern = "{0} is null",
            fields = listOf(stringField)
        )

        val condition2 = NativeSqlCondition(
            sqlPattern = "{0} is null",
            fields = listOf(stringField)
        )

        val condition3 = NativeSqlCondition(
            sqlPattern = "{0} is null",
            fields = listOf(intField)
        )

        assertEquals(condition1, condition2)
        assertEquals(condition1.hashCode(), condition2.hashCode())
        assertNotSame(condition1, condition2)

        assertNotEquals(condition1, condition3)
    }
}
