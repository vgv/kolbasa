package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import java.sql.PreparedStatement

abstract class Condition<Meta : Any> {

    internal abstract fun internalToSqlClause(queue: Queue<*, Meta>): String

    internal abstract fun internalFillPreparedQuery(
        queue: Queue<*, Meta>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    )

    // ------------------------------------------------------------

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

    internal fun fillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        checkQueueTheSame(queue)

        internalFillPreparedQuery(queue, preparedStatement, columnIndex)
    }

    internal fun findField(fieldName: String): MetaField<Meta> {
        return requireNotNull(linkedQueue.metadataDescription?.findMetaFieldByName(fieldName)) {
            "Field $fieldName not found in metadata class ${linkedQueue.metadata}"
        }
    }

    private fun checkQueueTheSame(queue: Queue<*, Meta>) {
        check(linkedQueue == queue) {
            "Queue $queue is not the same as $linkedQueue"
        }
    }

}

internal class ColumnIndex {
    private var index: Int = 1
    fun nextIndex() = index++
}
