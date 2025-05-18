package kolbasa.consumer.filter

import io.mockk.*
import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement
import kotlin.test.assertEquals

internal class OrConditionTest {

    @Test
    fun testSimpleToSql() {
        val expression = OrCondition(TestCondition("1"), TestCondition("2"))
        assertEquals("(1) or (2)", expression.toSqlClause(mockk()))
    }

    @Test
    fun testListAndObjectToSql() {
        val first = OrCondition(TestCondition("1"), TestCondition("2"))
        val second = TestCondition("3")
        val expression = OrCondition(first, second)

        assertEquals("(1) or (2) or (3)", expression.toSqlClause(mockk()))
    }

    @Test
    fun testObjectAndListToSql() {
        val first = TestCondition("1")
        val second = OrCondition(TestCondition("2"), TestCondition("3"))
        val expression = OrCondition(first, second)

        assertEquals("(1) or (2) or (3)", expression.toSqlClause(mockk()))
    }

    @Test
    fun testListAndListToSql() {
        val first = OrCondition(TestCondition("1"), TestCondition("2"))
        val second = OrCondition(TestCondition("3"), TestCondition("4"))
        val expression = OrCondition(first, second)

        assertEquals("(1) or (2) or (3) or (4)", expression.toSqlClause(mockk()))
    }

    @Test
    internal fun testFillPreparedQuery() {
        val firstCondition = mockk<Condition<String>>(relaxed = true)
        val secondCondition = mockk<Condition<String>>(relaxed = true)
        val thirdCondition = mockk<Condition<String>>(relaxed = true)

        val queue = mockk<Queue<*, String>>()
        val preparedStatement = mockk<PreparedStatement>()
        val column = mockk<ColumnIndex>()

        // make a call
        val orCondition = OrCondition(OrCondition(firstCondition, secondCondition), thirdCondition)
        orCondition.toSqlClause(queue)
        orCondition.fillPreparedQuery(
            queue,
            preparedStatement,
            column
        )

        // check
        verifySequence {
            firstCondition.toSqlClause(queue)
            secondCondition.toSqlClause(queue)
            thirdCondition.toSqlClause(queue)
            firstCondition.fillPreparedQuery(queue, preparedStatement, column)
            secondCondition.fillPreparedQuery(queue, preparedStatement, column)
            thirdCondition.fillPreparedQuery(queue, preparedStatement, column)
        }
        confirmVerified(firstCondition, secondCondition, thirdCondition)
    }
}
