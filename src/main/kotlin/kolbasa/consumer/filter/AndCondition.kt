package kolbasa.consumer.filter

import kolbasa.queue.Queue
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

    override fun internalToSqlClause(queue: Queue<*>): String {
        return conditions.joinToString(separator = " and ") {
            "(" + it.toSqlClause(queue) + ")"
        }
    }

    override fun internalFillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(queue, preparedStatement, columnIndex)
        }
    }

}
