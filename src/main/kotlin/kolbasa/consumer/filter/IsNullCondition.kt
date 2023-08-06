package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement

internal class IsNullCondition<Meta : Any>(private val fieldName: String) : Condition<Meta>() {

    override fun toSqlClause(queue: Queue<*, Meta>): String {
        val field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
            "Field $fieldName not found in metadata class ${queue.metadata}"
        }

        return "${field.dbColumnName} is null"
    }

    override fun fillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        // NOP
    }

}
