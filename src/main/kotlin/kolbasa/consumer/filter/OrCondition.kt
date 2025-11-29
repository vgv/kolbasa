package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class OrCondition(first: Condition, second: Condition) : Condition() {

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

    override fun toSqlClause(): String {
        return conditions.joinToString(separator = " or ") {
            "(" + it.toSqlClause() + ")"
        }
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(preparedStatement, columnIndex)
        }
    }

}
