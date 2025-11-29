package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class AndCondition(first: Condition, second: Condition) : Condition() {

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

    override fun toSqlClause(): String {
        return conditions.joinToString(separator = " and ") {
            "(" + it.toSqlClause() + ")"
        }
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(preparedStatement, columnIndex)
        }
    }

}
