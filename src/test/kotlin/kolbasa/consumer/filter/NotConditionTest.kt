package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verifySequence
import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class NotConditionTest {

    @Test
    fun testToSql() {
        val testExpression = TestCondition("test_expr")

        assertEquals("not (test_expr)", NotCondition(testExpression).toSqlClause(mockk()))
    }

    @Test
    fun testFillPreparedQuery() {
        val testCondition = mockk<Condition<String>>(relaxed = true)

        val queue = mockk<Queue<*, String>>()
        val preparedStatement = mockk<PreparedStatement>()
        val column = mockk<ColumnIndex>()

        // make a call
        val notCondition = NotCondition(testCondition)
        notCondition.toSqlClause(queue)
        notCondition.fillPreparedQuery(queue, preparedStatement, column)

        // check
        verifySequence {
            testCondition.toSqlClause(queue)
            testCondition.fillPreparedQuery(queue, preparedStatement, column)
        }
        confirmVerified(testCondition)
    }
}

