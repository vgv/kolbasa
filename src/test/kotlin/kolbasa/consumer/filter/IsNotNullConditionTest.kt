package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaHelpers
import kolbasa.utils.IntBox
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicInteger

internal class IsNotNullConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue("test_queue", dataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val isNotNullExpression = IsNotNullCondition<TestMeta>(TestMeta::intValue.name)

        val sql = isNotNullExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " is not null", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue("test_queue", dataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val isNotNullExpression = IsNotNullCondition<TestMeta>(TestMeta::intValue.name)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = IntBox(1)

        // call
        isNotNullExpression.toSqlClause(queue)
        isNotNullExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(val intValue: Int, val stringValue: String)
    }
}
