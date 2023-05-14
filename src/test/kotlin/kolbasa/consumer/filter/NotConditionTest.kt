package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicInteger

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
        val column = mockk<IntBox>()

        // make a call
        NotCondition(testCondition).fillPreparedQuery(queue, preparedStatement, column)

        // check
        verify { testCondition.fillPreparedQuery(queue, preparedStatement, column) }
        confirmVerified(testCondition)
    }
}

