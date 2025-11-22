package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaHelpers
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class BetweenConditionTest {

    @Test
    fun testToSql() {
        val betweenExpression = BetweenCondition(INT_VALUE, Pair(10, 20))

        val sql = betweenExpression.toSqlClause()
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " between ? and ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val betweenExpression = BetweenCondition(INT_VALUE, Pair(10, 20))

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        betweenExpression.toSqlClause()
        betweenExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(10)) }
        verify { preparedStatement.setInt(eq(2), eq(20)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCH)
    }
}
