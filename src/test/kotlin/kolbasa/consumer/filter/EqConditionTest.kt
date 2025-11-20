package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.*
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class EqConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue.of(
            "test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = Metadata.of(INT_VALUE, STRING_VALUE)
        )
        val eqExpression = EqCondition(INT_VALUE, 123)

        val sql = eqExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " = ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue.of(
            "test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = Metadata.of(INT_VALUE, STRING_VALUE)
        )
        val eqExpression = EqCondition(INT_VALUE, 123)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        eqExpression.toSqlClause(queue)
        eqExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(123)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCHABLE)
        private val STRING_VALUE = MetaField.string("string_value")
    }
}
