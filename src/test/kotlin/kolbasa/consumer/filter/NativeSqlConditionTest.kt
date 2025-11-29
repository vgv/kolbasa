package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement
import kotlin.test.assertEquals

class NativeSqlConditionTest {
    @Test
    fun testToSql() {
        val nativeExpression = NativeSqlCondition(
            sqlPattern = "({0} like ''a%'') or ({0} like ''b%'') and {1} is null and {2} is null",
            fields = arrayOf(STRING_VALUE, INT_VALUE, STRING_VALUE)
        )

        val sql = nativeExpression.toSqlClause()
        assertEquals(
            expected = "(meta_string_value like 'a%') or (meta_string_value like 'b%') and meta_int_value is null and meta_string_value is null",
            actual = sql
        )
    }

    @Test
    fun testFillPreparedQuery() {
        val nativeExpression = NativeSqlCondition(
            sqlPattern = "{0} is null",
            fields = arrayOf(STRING_VALUE)
        )
        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        nativeExpression.toSqlClause()
        nativeExpression.fillPreparedQuery(preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCH)
        private val STRING_VALUE = MetaField.string("string_value", FieldOption.SEARCH)
    }
}
