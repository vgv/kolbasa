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

internal class AndConditionTest {

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
        val expression = AndCondition(idCondition, emailCondition)

        assertEquals("(${idCondition.toSqlClause()}) and (${emailCondition.toSqlClause()})", expression.toSqlClause())
    }

    @Test
    fun testListAndObjectToSql() {
        // ((1 and 2) and 3) converts to (1 and 2 and 3)
        val expression = AndCondition(
            AndCondition(idCondition, emailCondition),
            birthCondition
        )

        assertEquals(
            "(${idCondition.toSqlClause()}) and (${emailCondition.toSqlClause()}) and (${birthCondition.toSqlClause()})",
            expression.toSqlClause()
        )
    }

    @Test
    fun testObjectAndListToSql() {
        // (1 and (2 and 3)) converts to (1 and 2 and 3)
        val expression = AndCondition(
            idCondition,
            AndCondition(emailCondition, birthCondition)
        )

        assertEquals(
            "(${idCondition.toSqlClause()}) and (${emailCondition.toSqlClause()}) and (${birthCondition.toSqlClause()})",
            expression.toSqlClause()
        )
    }

    @Test
    fun testListAndListToSql() {
        // ((1 and 2) and (3 and 4)) converts to (1 and 2 and 3 and 4)
        val expression = AndCondition(
            AndCondition(idCondition, emailCondition),
            AndCondition(birthCondition, ageCondition)
        )

        assertEquals(
            "(${idCondition.toSqlClause()}) and (${emailCondition.toSqlClause()}) and (${birthCondition.toSqlClause()}) and (${ageCondition.toSqlClause()})",
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

