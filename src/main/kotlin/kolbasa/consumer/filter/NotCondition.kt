package kolbasa.consumer.filter

import kolbasa.queue.Queue
import java.sql.PreparedStatement

internal data class NotCondition<Meta : Any>(val condition: Condition<Meta>) : Condition<Meta>() {

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        return "not (${condition.toSqlClause(queue)})"
    }

    override fun internalFillPreparedQuery(
        queue: Queue<*, Meta>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    ) {
        condition.fillPreparedQuery(queue, preparedStatement, columnIndex)
    }

}
