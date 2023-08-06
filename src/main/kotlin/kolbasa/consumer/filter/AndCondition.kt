package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal class AndCondition<Meta : Any>(first: Condition<Meta>, second: Condition<Meta>) : Condition<Meta>() {

    private val conditions: List<Condition<Meta>> = when {
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

    override fun toSqlClause(queue: Queue<*, Meta>): String {
        return conditions.joinToString(separator = " and ") {
            "(" + it.toSqlClause(queue) + ")"
        }
    }

    override fun fillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(queue, preparedStatement, columnIndex)
        }
    }
}
