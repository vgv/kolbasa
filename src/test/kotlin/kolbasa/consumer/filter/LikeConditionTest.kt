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

internal class LikeConditionTest {

    @Test
    fun testToSql() {
        val queue = Queue.of("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val likeExpression = LikeCondition<TestMeta>(TestMeta::stringValue.name, "123%")

        val sql = likeExpression.toSqlClause(queue)
        assertEquals(MetaHelpers.generateMetaColumnName("stringValue") + " like ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val queue = Queue.of("test_queue", databaseDataType = PredefinedDataTypes.ByteArray, metadata = TestMeta::class.java)
        val likeExpression = LikeCondition<TestMeta>(TestMeta::stringValue.name, "123%")

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        likeExpression.toSqlClause(queue)
        likeExpression.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify { preparedStatement.setString(eq(1), eq("123%")) }
        confirmVerified(preparedStatement)
    }

    companion object {
        data class TestMeta(
            @Searchable val intValue: Int,
            @Searchable val stringValue: String
        )
    }
}
