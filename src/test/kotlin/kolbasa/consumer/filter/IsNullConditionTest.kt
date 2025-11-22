package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaHelpers
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class IsNullConditionTest {

    @Test
    fun testToSql() {
        val isNullExpression = IsNullCondition(INT_VALUE)

        val sql = isNullExpression.toSqlClause()
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " is null", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val isNullExpression = IsNullCondition(INT_VALUE)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        isNullExpression.toSqlClause()
        isNullExpression.fillPreparedQuery(preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCHABLE)
    }
}
