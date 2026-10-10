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

internal class EqConditionTest {

    private val intField = MetaField.ofInt("int_value", FieldOption.SEARCH)

    @Test
    fun testToSql() {
        val eqExpression = EqCondition(intField, 123)

        val sql = eqExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("intValue") + "=?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val eqExpression = EqCondition(intField, 123)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        eqExpression.toSqlClause()
        eqExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(123)) }
        confirmVerified(preparedStatement)
    }

    @Test
    fun testEqualsAndHashCode() {
        val condition1 = EqCondition(intField, 123)
        val condition2 = EqCondition(intField, 123)
        val condition3 = EqCondition(intField, 456)

        assertEquals(condition1, condition2)
        assertEquals(condition1.hashCode(), condition2.hashCode())
        assertNotSame(condition1, condition2)

        assertNotEquals(condition1, condition3)
    }
}
