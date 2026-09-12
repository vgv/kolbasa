package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class IsNullConditionTest {

    private val intField = MetaField.ofInt("int_value", FieldOption.SEARCH)
    private val strField = MetaField.ofString("str_value", FieldOption.SEARCH)

    @Test
    fun testToSql() {
        val isNullExpression = IsNullCondition(intField)

        val sql = isNullExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("intValue") + " is null", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val isNullExpression = IsNullCondition(intField)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        isNullExpression.toSqlClause()
        isNullExpression.fillPreparedQuery(preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    @Test
    fun testEqualsAndHashCode() {
        val condition1 = IsNullCondition(intField)
        val condition2 = IsNullCondition(intField)
        val condition3 = IsNullCondition(strField)

        assertEquals(condition1, condition2)
        assertEquals(condition1.hashCode(), condition2.hashCode())
        assertNotSame(condition1, condition2)

        assertNotEquals(condition1, condition3)
    }

}
