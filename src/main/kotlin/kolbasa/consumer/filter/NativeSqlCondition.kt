package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.IntBox
import java.sql.PreparedStatement
import java.text.MessageFormat

internal class NativeSqlCondition<M : Any>(
    private val sqlPattern: String,
    private val fieldNames: List<String>
) : Condition<M>() {

    override fun toSqlClause(queue: Queue<*, M>): String {
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

    override fun fillPreparedQuery(queue: Queue<*, M>, preparedStatement: PreparedStatement, columnIndex: IntBox) {
        // NOP
    }
}
