package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal class OrCondition<M: Any>(first: Condition<M>, second: Condition<M>) : Condition<M>() {

    private val conditions: List<Condition<M>> = when {
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

    override fun toSqlClause(queue: Queue<*, M>): String {
        return conditions.joinToString(separator = " or ") {
            "(" + it.toSqlClause(queue) + ")"
        }
    }

    override fun fillPreparedQuery(queue: Queue<*, M>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(queue, preparedStatement, columnIndex)
        }
    }
}
