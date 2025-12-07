package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.*
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class LessThanOrEqConditionTest {

    @Test
    fun testToSql() {
        val lteExpression = LessThanOrEqCondition(INT_VALUE, 123)

        val sql = lteExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("intValue") + " <= ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val lteExpression = LessThanOrEqCondition(INT_VALUE, 123)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        lteExpression.toSqlClause()
        lteExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(123)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCH)
    }
}
