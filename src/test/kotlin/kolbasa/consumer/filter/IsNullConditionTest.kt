package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.*
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class IsNullConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue.of(
            "test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = Metadata.of(INT_VALUE, STRING_VALUE)
        )

        val isNullExpression = IsNullCondition(INT_VALUE)

        val sql = isNullExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " is null", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue.of(
            "test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = Metadata.of(INT_VALUE, STRING_VALUE)
        )

        val isNullExpression = IsNullCondition(INT_VALUE)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        isNullExpression.toSqlClause(queue)
        isNullExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    companion object {
        private val INT_VALUE = MetaField.int("int_value", FieldOption.SEARCHABLE)
        private val STRING_VALUE = MetaField.string("string_value")
    }
}
