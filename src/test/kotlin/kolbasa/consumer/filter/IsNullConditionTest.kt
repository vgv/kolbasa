package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.queue.meta.MetaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class IsNullConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val isNullExpression = IsNullCondition<TestMeta>(TestMeta::intValue.name)

        val sql = isNullExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " is null", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val isNullExpression = IsNullCondition<TestMeta>(TestMeta::intValue.name)

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        isNullExpression.toSqlClause(queue)
        isNullExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(
            @Searchable val intValue: Int,
            val stringValue: String
        )
    }
}
