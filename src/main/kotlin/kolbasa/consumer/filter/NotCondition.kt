package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal data class NotCondition<M: Any>(val condition: Condition<M>) : Condition<M>() {

    override fun toSqlClause(queue: Queue<*, M>): String {
        return "not (${condition.toSqlClause(queue)})"
    }

    override fun fillPreparedQuery(queue: Queue<*, M>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        condition.fillPreparedQuery(queue, preparedStatement, columnIndex)
    }
}
