package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

abstract class Condition<Meta : Any> {

    internal abstract fun toSqlClause(queue: Queue<*, Meta>): String

    internal abstract fun fillPreparedQuery(
        queue: Queue<*, Meta>,
        preparedStatement: PreparedStatement,
        columnIndex: IntBox
    )
}

