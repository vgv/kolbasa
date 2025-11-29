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

internal class IsNotNullConditionTest {

    @Test
    fun testToSql() {
        val isNotNullExpression = IsNotNullCondition(INT_VALUE)

        val sql = isNotNullExpression.toSqlClause()
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " is not null", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val isNotNullExpression = IsNotNullCondition(INT_VALUE)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        isNotNullExpression.toSqlClause()
        isNotNullExpression.fillPreparedQuery(preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCH)
    }
}
