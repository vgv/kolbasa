package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal abstract class AbstractOneValueCondition<Meta : Any, T>(
    private val fieldName: String,
    private val value: T
) : Condition<Meta>() {

    private lateinit var field: MetaField<Meta>

    abstract val operator: String

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::field.isInitialized) {
            field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
                "Field $fieldName not found in metadata class ${queue.metadata}"
            }
        }

        // Field Operator Parameter, like field=?, field>? etc.
        return "${field.dbColumnName}${operator}?"
    }

    override fun internalFillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.getAndIncrement(), value)
    }

}
