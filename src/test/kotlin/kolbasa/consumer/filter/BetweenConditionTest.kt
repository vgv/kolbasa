package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.queue.meta.MetaHelpers
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class BetweenConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue.of("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val betweenExpression = BetweenCondition<TestMeta, Int>(TestMeta::intValue.name, Pair(10, 20))

        val sql = betweenExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " between ? and ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue.of("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val betweenExpression = BetweenCondition<TestMeta, Int>(TestMeta::intValue.name, Pair(10, 20))

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        betweenExpression.toSqlClause(queue)
        betweenExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(10)) }
        verify { preparedStatement.setInt(eq(2), eq(20)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(
            @Searchable val intValue: Int,
            val stringValue: String
        )
    }
}
