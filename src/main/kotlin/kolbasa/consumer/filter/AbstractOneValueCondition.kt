package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal abstract class AbstractOneValueCondition<M : Any, T>(
    private val fieldName: String,
    private val value: T
) : Condition<M>() {

    private lateinit var field: MetaField<M>

    abstract val operator: String

    override fun toSqlClause(queue: Queue<*, M>): String {
        field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
            "Field $fieldName not found in metadata class ${queue.metadata}"
        }

        // Field Operator Parameter, like field=?, field>? etc.
        return "${field.dbColumnName}${operator}?"
    }

    override fun fillPreparedQuery(queue: Queue<*, M>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.getAndIncrement(), value)
    }

}
