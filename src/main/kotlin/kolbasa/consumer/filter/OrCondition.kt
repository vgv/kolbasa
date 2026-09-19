package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class OrCondition(val first: Condition, val second: Condition) : Condition() {

    private val conditions: List<Condition> = when {
        first is OrCondition && second is OrCondition -> {
            first.conditions + second.conditions
        }

        first is OrCondition -> {
            first.conditions + second
        }

        second is OrCondition -> {
            listOf(first) + second.conditions
        }

        else -> {
            listOf(first, second)
        }
    }

    private val sqlClause = conditions.joinToString(separator = " or ") {
        "(" + it.toSqlClause() + ")"
    }

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(preparedStatement, columnIndex)
        }
    }

}
