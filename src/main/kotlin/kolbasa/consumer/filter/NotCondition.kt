package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal data class NotCondition<Meta : Any>(val condition: Condition<Meta>) : Condition<Meta>() {

    override fun toSqlClause(queue: Queue<*, Meta>): String {
        return "not (${condition.toSqlClause(queue)})"
    }

    override fun fillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        condition.fillPreparedQuery(queue, preparedStatement, columnIndex)
    }
}
