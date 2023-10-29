package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

abstract class Condition<Meta : Any> {

    // Every condition is 'linked' to some queue, but linking is done lately,
    // after first call to toSqlClause() method
    private lateinit var linkedQueue: Queue<*, Meta>

    internal fun toSqlClause(queue: Queue<*, Meta>): String {
        if (!::linkedQueue.isInitialized) {
            linkedQueue = queue
        }
        checkQueueTheSame(queue)

        return internalToSqlClause(queue)
    }

    internal fun fillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        checkQueueTheSame(queue)

        internalFillPreparedQuery(queue, preparedStatement, columnIndex)
    }

    internal abstract fun internalToSqlClause(queue: Queue<*, Meta>): String

    internal abstract fun internalFillPreparedQuery(
        queue: Queue<*, Meta>,
        preparedStatement: PreparedStatement,
        columnIndex: IntBox
    )


    private fun checkQueueTheSame(queue: Queue<*, Meta>) = check(linkedQueue == queue) {
        "Queue $queue is not the same as $linkedQueue"
    }

}

