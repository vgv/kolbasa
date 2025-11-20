package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.FieldOption
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

abstract class Condition {

    internal abstract fun internalToSqlClause(queue: Queue<*>): String

    internal abstract fun internalFillPreparedQuery(
        queue: Queue<*>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    )

    // ------------------------------------------------------------

    // Every condition is 'linked' to some queue, but linking is done lately,
    // after first call to toSqlClause() method
    private lateinit var linkedQueue: Queue<*>

    internal fun toSqlClause(queue: Queue<*>): String {
        if (!::linkedQueue.isInitialized) {
            linkedQueue = queue
        }
        checkQueueTheSame(queue)

        return internalToSqlClause(queue)
    }

    internal fun fillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        checkQueueTheSame(queue)

        internalFillPreparedQuery(queue, preparedStatement, columnIndex)
    }

    internal fun findField(fieldName: String): MetaField<*> {
        val field = requireNotNull(linkedQueue.metadata.findMetaFieldByName(fieldName)) {
            "Field $fieldName not found in metadata class ${linkedQueue.metadata}"
        }

        // Check that field is searchable or unique
        check(field.option == FieldOption.SEARCHABLE || field.option == FieldOption.UNIQUE_SEARCHABLE) {
            "Field $fieldName is not SEARCHABLE or UNIQUE_SEARCHABLE, but you are trying to use it in condition"
        }

        return field
    }

    private fun checkQueueTheSame(queue: Queue<*>) {
        check(linkedQueue == queue) {
            "Queue $queue is not the same as $linkedQueue"
        }
    }

}

