package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class TestCondition(val expr: String) : Condition() {

    override fun internalToSqlClause(queue: Queue<*>): String {
        return expr
    }

    override fun internalFillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // nothing to do
    }
}
