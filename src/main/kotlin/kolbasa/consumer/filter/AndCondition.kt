package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal class AndCondition<M: Any>(first: Condition<M>, second: Condition<M>) : Condition<M>() {

    private val conditions: List<Condition<M>> = when {
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

    override fun toSqlClause(queue: Queue<*, M>): String {
        return conditions.joinToString(separator = " and ") {
            "(" + it.toSqlClause(queue) + ")"
        }
    }

    override fun fillPreparedQuery(queue: Queue<*, M>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(queue, preparedStatement, columnIndex)
        }
    }
}
