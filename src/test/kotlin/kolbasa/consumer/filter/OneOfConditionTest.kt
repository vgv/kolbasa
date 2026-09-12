package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class OneOfConditionTest {

    private val intField = MetaField.ofInt("int_value", FieldOption.SEARCH)

    @Test
    fun testToSql() {
        val oneOfExpression = OneOfCondition(intField, listOf(123))

        val sql = oneOfExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("intValue") + " = ANY (?)", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val oneOfExpression = OneOfCondition(intField, listOf(123))

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

    @Test
    fun testEqualsAndHashCode() {
        val condition1 = OneOfCondition(intField, listOf(123))
        val condition2 = OneOfCondition(intField, listOf(123))
        val condition3 = OneOfCondition(intField, listOf(456))

        assertEquals(condition1, condition2)
        assertEquals(condition1.hashCode(), condition2.hashCode())
        assertNotSame(condition1, condition2)

        assertNotEquals(condition1, condition3)
    }

}
