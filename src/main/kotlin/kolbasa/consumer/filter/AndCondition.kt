package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class AndCondition(val first: Condition, val second: Condition) : Condition() {

    private val conditions: List<Condition> = when {
        first is AndCondition && second is AndCondition -> {
            first.conditions + second.conditions
        }

        first is AndCondition -> {
            first.conditions + second
        }

        second is AndCondition -> {
            listOf(first) + second.conditions
        }

        else -> {
            listOf(first, second)
        }
    }

    private val sqlClause = conditions.joinToString(separator = " and ") {
        "(" + it.toSqlClause() + ")"
    }

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(preparedStatement, columnIndex)
        }
    }

}
