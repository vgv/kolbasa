package kolbasa.consumer.filter

import kolbasa.queue.Queue
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

    override fun internalToSqlClause(queue: Queue<*>): String {
        return conditions.joinToString(separator = " or ") {
            "(" + it.toSqlClause(queue) + ")"
        }
    }

    override fun internalFillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(queue, preparedStatement, columnIndex)
        }
    }

}
