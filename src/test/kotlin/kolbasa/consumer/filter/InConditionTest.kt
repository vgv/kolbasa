package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.queue.meta.MetaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class InConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue.of("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val inExpression = InCondition<TestMeta, Int>(TestMeta::intValue.name, listOf(123))

        val sql = inExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("intValue") + " = ANY (?)", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue.of("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val inExpression = InCondition<TestMeta, Int>(TestMeta::intValue.name, listOf(123))

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        inExpression.toSqlClause(queue)
        inExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify {
            preparedStatement.connection.createArrayOf("int", arrayOf(123))
            preparedStatement.setArray(eq(1), any())
        }
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(
            @Searchable val intValue: Int,
            val stringValue: String
        )
    }
}
