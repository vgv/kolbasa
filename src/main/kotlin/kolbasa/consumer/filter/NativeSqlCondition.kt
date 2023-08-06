package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement
import java.text.MessageFormat

internal class NativeSqlCondition<Meta : Any>(
    private val sqlPattern: String,
    private val fieldNames: List<String>
) : Condition<Meta>() {

    override fun toSqlClause(queue: Queue<*, Meta>): String {
        val names = Array(fieldNames.size) {
            val fieldName = fieldNames[it]
            val field = requireNotNull(queue.metadataDescription?.findMetaFieldByName(fieldName)) {
                "Field $fieldName not found in metadata class ${queue.metadata}"
            }
            field.dbColumnName
        }

        // make a replacement
        return MessageFormat.format(sqlPattern, *names)
    }

    override fun fillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        // NOP
    }
}
