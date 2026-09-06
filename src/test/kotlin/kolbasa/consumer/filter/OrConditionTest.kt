package kolbasa.consumer.filter

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verifySequence
import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.greaterEq
import kolbasa.consumer.filter.Filter.like
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.PreparedStatement
import java.time.Instant
import java.time.temporal.ChronoUnit

internal class OrConditionTest {

    private val now = Instant.now()
    private val then = now.plus(1, ChronoUnit.DAYS)

    private val userId = MetaField.ofInt("user_id")
    private val userEmail = MetaField.ofString("user_email")
    private val userBirth = MetaField.ofInstant("user_birth")
    private val userAge = MetaField.ofByte("user_age")

    private val idCondition = userId eq 42
    private val emailCondition = userEmail like "kolbasa%"
    private val birthCondition = userBirth between now and then
    private val ageCondition = userAge greaterEq 21

    @Test
    fun testSimpleToSql() {
        val expression = OrCondition(idCondition, emailCondition)
        assertEquals("(${idCondition.toSqlClause()}) or (${emailCondition.toSqlClause()})", expression.toSqlClause())
    }

    @Test
    fun testListAndObjectToSql() {
        // ((1 or 2) or 3) converts to (1 or 2 or 3)
        val expression = OrCondition(
            OrCondition(idCondition, emailCondition),
            birthCondition
        )

        assertEquals(
            "(${idCondition.toSqlClause()}) or (${emailCondition.toSqlClause()}) or (${birthCondition.toSqlClause()})",
            expression.toSqlClause()
        )
    }

    @Test
    fun testObjectAndListToSql() {
        // (1 or (2 or 3)) converts to (1 or 2 or 3)
        val expression = OrCondition(
            idCondition,
            OrCondition(emailCondition, birthCondition)
        )

        assertEquals(
            "(${idCondition.toSqlClause()}) or (${emailCondition.toSqlClause()}) or (${birthCondition.toSqlClause()})",
            expression.toSqlClause()
        )
    }

    @Test
    fun testListAndListToSql() {
        // ((1 or 2) or (3 or 4)) converts to (1 or 2 or 3 or 4)
        val expression = OrCondition(
            OrCondition(idCondition, emailCondition),
            OrCondition(birthCondition, ageCondition)
        )

        assertEquals(
            "(${idCondition.toSqlClause()}) or (${emailCondition.toSqlClause()}) or (${birthCondition.toSqlClause()}) or (${ageCondition.toSqlClause()})",
            expression.toSqlClause()
        )
    }

    @Test
    internal fun testFillPreparedQuery() {
        val firstCondition = mockk<Condition>(relaxed = true)
        val secondCondition = mockk<Condition>(relaxed = true)
        val thirdCondition = mockk<Condition>(relaxed = true)

        val preparedStatement = mockk<PreparedStatement>()
        val column = mockk<ColumnIndex>()

        // make a call
        val orCondition = OrCondition(OrCondition(firstCondition, secondCondition), thirdCondition)
        orCondition.toSqlClause()
        orCondition.fillPreparedQuery(preparedStatement, column)

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
