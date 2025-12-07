package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaHelpers
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement

internal class LikeConditionTest {

    @Test
    fun testToSql() {
        val likeExpression = LikeCondition(STRING_VALUE, "123%")

        val sql = likeExpression.toSqlClause()
        assertEquals(QueueHelpers.generateMetaColumnDbName("stringValue") + " like ?", sql)
    }

    @Test
    fun testFillPreparedQuery() {
        val likeExpression = LikeCondition(STRING_VALUE, "123%")

        val preparedStatement = mockk<PreparedStatement>(relaxed = true)
        val column = ColumnIndex()

        // call
        likeExpression.toSqlClause()
        likeExpression.fillPreparedQuery(preparedStatement, column)

        // check
        verify { preparedStatement.setString(eq(1), eq("123%")) }
        confirmVerified(preparedStatement)
    }

    companion object {
        private val STRING_VALUE = MetaField.string("string_value", FieldOption.SEARCH)
    }
}
