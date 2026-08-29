package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verifySequence
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class NotConditionTest {

    private val userId = MetaField.ofInt("user_id")

    @Test
    fun testToSql() {
        val expression = EqCondition(userId, 42)

        assertEquals("not (${expression.toSqlClause()})", NotCondition(expression).toSqlClause())
    }

    @Test
    fun testFillPreparedQuery() {
        val testCondition = mockk<Condition>(relaxed = true)

        val preparedStatement = mockk<PreparedStatement>()
        val column = mockk<ColumnIndex>()

        // make a call
        val notCondition = NotCondition(testCondition)
        notCondition.toSqlClause()
        notCondition.fillPreparedQuery(preparedStatement, column)

        // check
        verifySequence {
            testCondition.toSqlClause()
            testCondition.fillPreparedQuery(preparedStatement, column)
        }
        confirmVerified(testCondition)
    }
}

