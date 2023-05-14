package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

abstract class Condition<META : Any> {

    internal abstract fun toSqlClause(queue: Queue<*, META>): String

    internal abstract fun fillPreparedQuery(
        queue: Queue<*, META>,
        preparedStatement: PreparedStatement,
        columnIndex: IntBox
    )
}

