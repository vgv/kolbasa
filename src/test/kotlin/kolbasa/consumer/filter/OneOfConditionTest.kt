package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class OneOfConditionTest {

    @Test
    fun testToSql() {
        val oneOfExpression = OneOfCondition(INT_VALUE, listOf(123))

        val sql = oneOfExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("intValue") + " = ANY (?)", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val oneOfExpression = OneOfCondition(INT_VALUE, listOf(123))

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        oneOfExpression.toSqlClause()
        oneOfExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify {
            preparedStatement.connection.createArrayOf("int", arrayOf(123))
            preparedStatement.setArray(eq(1), any())
        }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.ofInt("int_value", FieldOption.SEARCH)
    }
}
