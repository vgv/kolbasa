package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import java.sql.PreparedStatement

internal data class IsNotNullCondition<Meta : Any>(private val fieldName: String) : Condition<Meta>() {

    private lateinit var field: MetaField<Meta>

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::field.isInitialized) {
            field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
                "Field $fieldName not found in metadata class ${queue.metadata}"
            }
        }

        return "${field.dbColumnName} is not null"
    }

    override fun internalFillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
