package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class OrCondition<Meta : Any>(first: Condition<Meta>, second: Condition<Meta>) : Condition<Meta>() {

    private val conditions: List<Condition<Meta>> = when {
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

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        return conditions.joinToString(separator = " or ") {
            "(" + it.toSqlClause(queue) + ")"
        }
    }

    override fun internalFillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        conditions.forEach { expression ->
            expression.fillPreparedQuery(queue, preparedStatement, columnIndex)
        }
    }

}
