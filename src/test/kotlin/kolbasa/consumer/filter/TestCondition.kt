package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal data class TestCondition(val expr: String) : Condition<String>() {

    override fun toSqlClause(queue: Queue<*, String>): String {
        return expr
    }

    override fun fillPreparedQuery(
        queue: Queue<*, String>,
        preparedStatement: PreparedStatement,
        columnIndex: IntBox
    ) {

    }
}
