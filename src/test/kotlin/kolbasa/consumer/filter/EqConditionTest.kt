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

internal class EqConditionTest {

    @Test
    fun testToSql() {
        val eqExpression = EqCondition(INT_VALUE, 123)

        val sql = eqExpression.toSqlClause()
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " = ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val eqExpression = EqCondition(INT_VALUE, 123)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        eqExpression.toSqlClause()
        eqExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(123)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCHABLE)
    }
}
