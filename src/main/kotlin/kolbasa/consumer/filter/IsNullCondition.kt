package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal class IsNullCondition<M: Any>(private val fieldName: String) : Condition<M>(){

    override fun toSqlClause(queue: Queue<*, M>): String {
        val field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
            "Field $fieldName not found in metadata class ${queue.metadata}"
        }

        return "${field.dbColumnName} is null"
    }

    override fun fillPreparedQuery(queue: Queue<*, M>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        // NOP
    }

}
