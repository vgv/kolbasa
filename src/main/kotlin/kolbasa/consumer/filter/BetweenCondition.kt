package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal class BetweenCondition<Meta : Any, T>(
    private val fieldName: String,
    private val value: Pair<T, T>
) : Condition<Meta>() {

    private lateinit var field: MetaField<Meta>

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::field.isInitialized) {
            field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
                "Field $fieldName not found in metadata class ${queue.metadata}"
            }
        }

        return "${field.dbColumnName} between ? and ?"
    }

    override fun internalFillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.getAndIncrement(), value.first)
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.getAndIncrement(), value.second)
    }

}
