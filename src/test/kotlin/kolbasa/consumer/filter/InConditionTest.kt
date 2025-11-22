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

internal class InConditionTest {

    @Test
    fun testToSql() {
        val inExpression = InCondition(INT_VALUE, listOf(123))

        val sql = inExpression.toSqlClause()
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " = ANY (?)", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val inExpression = InCondition(INT_VALUE, listOf(123))

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        inExpression.toSqlClause()
        inExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify {
            preparedStatement.connection.createArrayOf("int", arrayOf(123))
            preparedStatement.setArray(eq(1), any())
        }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCH)
    }
}
