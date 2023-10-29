package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class GreaterThanConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val gtExpression = GreaterThanCondition<TestMeta, Int>(TestMeta::intValue.name, 123)

        val sql = gtExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + ">?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val gtExpression = GreaterThanCondition<TestMeta, Int>(TestMeta::intValue.name, 123)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        gtExpression.toSqlClause(queue)
        gtExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(123)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(val intValue: Int, val stringValue: String)
    }
}
