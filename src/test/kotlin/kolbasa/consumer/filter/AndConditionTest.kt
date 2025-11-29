package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verifySequence
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class AndConditionTest {

    @Test
    fun testSimpleToSql() {
        val expression =  AndCondition(TestCondition("1"), TestCondition("2"))
        //val queue =

        kotlin.test.assertEquals("(1) and (2)", expression.toSqlClause())
    }

    @Test
    fun testListAndObjectToSql() {
        val first = AndCondition(TestCondition("1"), TestCondition("2"))
        val second = TestCondition("3")
        val expression = AndCondition(first, second)

        kotlin.test.assertEquals("(1) and (2) and (3)", expression.toSqlClause())
    }
    @Test
    fun testObjectAndListToSql() {
        val first = TestCondition("1")
        val second = AndCondition(TestCondition("2"), TestCondition("3"))
        val expression = AndCondition(first, second)

        kotlin.test.assertEquals("(1) and (2) and (3)", expression.toSqlClause())
    }

    @Test
    fun testListAndListToSql() {
        val first = AndCondition(TestCondition("1"), TestCondition("2"))
        val second = AndCondition(TestCondition("3"), TestCondition("4"))
        val expression = AndCondition(first, second)

        kotlin.test.assertEquals("(1) and (2) and (3) and (4)", expression.toSqlClause())
    }

    @Test
    internal fun testFillPreparedQuery() {
        val firstCondition = mockk<Condition>(relaxed = true)
        val secondCondition = mockk<Condition>(relaxed = true)
        val thirdCondition = mockk<Condition>(relaxed = true)

        val preparedStatement = mockk<PreparedStatement>()
        val column = mockk<ColumnIndex>()

        // make a call
        val andCondition = AndCondition(AndCondition(firstCondition, secondCondition), thirdCondition)
        andCondition.toSqlClause() // to initialize queue
        andCondition.fillPreparedQuery(preparedStatement, column)

        // check
        verifySequence {
            firstCondition.toSqlClause()
            secondCondition.toSqlClause()
            thirdCondition.toSqlClause()
            firstCondition.fillPreparedQuery(preparedStatement, column)
            secondCondition.fillPreparedQuery(preparedStatement, column)
            thirdCondition.fillPreparedQuery(preparedStatement, column)
        }
        confirmVerified(firstCondition, secondCondition, thirdCondition)
    }

}

