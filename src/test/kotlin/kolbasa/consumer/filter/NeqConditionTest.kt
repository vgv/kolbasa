package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaHelpers
import kolbasa.utils.IntBox
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicInteger

internal class NeqConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue("test_queue", dataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val neqExpression = NeqCondition<TestMeta, Int>(TestMeta::intValue.name, 123)

        val sql = neqExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + "<>?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue("test_queue", dataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val neqExpression = NeqCondition<TestMeta, Int>(TestMeta::intValue.name, 123)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = IntBox(1)

        // call
        neqExpression.toSqlClause(queue)
        neqExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify { preparedStatement.setInt(eq(1), eq(123)) }
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(val intValue: Int, val stringValue: String)
    }
}
