package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class NotCondition(val condition: Condition) : Condition() {

    override fun internalToSqlClause(queue: Queue<*>): String {
        return "not (${condition.toSqlClause(queue)})"
    }

    override fun internalFillPreparedQuery(
        queue: Queue<*>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    ) {
        condition.fillPreparedQuery(queue, preparedStatement, columnIndex)
    }

}
