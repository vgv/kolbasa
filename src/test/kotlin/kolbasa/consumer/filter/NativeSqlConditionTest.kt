package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

class NativeSqlConditionTest {

    private val intField = MetaField.ofInt("int_value", FieldOption.SEARCH)
    private val stringField = MetaField.ofString("string_value", FieldOption.SEARCH)

    @Test
    fun testToSql() {
        val nativeExpression = NativeSqlCondition(
            sqlPattern = "({0} like ''a%'') or ({0} like ''b%'') and {1} is null and {2} is null",
            fields = listOf(stringField, intField, stringField)
        )

        val sql = nativeExpression.toSqlClause()
        assertEquals(
            "(meta_string_value like 'a%') or (meta_string_value like 'b%') and meta_int_value is null and meta_string_value is null",
            sql
        )
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
