package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class LikeConditionTest {

    private val strField = MetaField.ofString("string_value", FieldOption.SEARCH)

    @Test
    fun testToSql() {
        val likeExpression = LikeCondition(strField, "123%")

        val sql = likeExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("stringValue") + " like ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val likeExpression = LikeCondition(strField, "123%")

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        likeExpression.toSqlClause()
        likeExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify { preparedStatement.setString(eq(1), eq("123%")) }
        confirmVerified(preparedStatement)
    }

    @Test
    fun testEqualsAndHashCode() {
        val condition1 = LikeCondition(strField, "123")
        val condition2 = LikeCondition(strField, "123")
        val condition3 = LikeCondition(strField, "456")

        assertEquals(condition1, condition2)
        assertEquals(condition1.hashCode(), condition2.hashCode())
        assertNotSame(condition1, condition2)

        assertNotEquals(condition1, condition3)
    }

}
