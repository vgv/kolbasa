package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verifySequence
import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class AndConditionTest {

    @Test
    fun testSimpleToSql() {
        val expression =  AndCondition(TestCondition("1"), TestCondition("2"))
        //val queue =

        kotlin.test.assertEquals("(1) and (2)", expression.toSqlClause(mockk()))
    }

    @Test
    fun testListAndObjectToSql() {
        val first = AndCondition(TestCondition("1"), TestCondition("2"))
        val second = TestCondition("3")
        val expression = AndCondition(first, second)

        kotlin.test.assertEquals("(1) and (2) and (3)", expression.toSqlClause(mockk()))
    }
    @Test
    fun testObjectAndListToSql() {
        val first = TestCondition("1")
        val second = AndCondition(TestCondition("2"), TestCondition("3"))
        val expression = AndCondition(first, second)

        kotlin.test.assertEquals("(1) and (2) and (3)", expression.toSqlClause(mockk()))
    }

    @Test
    fun testListAndListToSql() {
        val first = AndCondition(TestCondition("1"), TestCondition("2"))
        val second = AndCondition(TestCondition("3"), TestCondition("4"))
        val expression = AndCondition(first, second)

        kotlin.test.assertEquals("(1) and (2) and (3) and (4)", expression.toSqlClause(mockk()))
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
        val andCondition = AndCondition(AndCondition(firstCondition, secondCondition), thirdCondition)
        andCondition.toSqlClause(queue) // to initialize queue
        andCondition.fillPreparedQuery(
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

