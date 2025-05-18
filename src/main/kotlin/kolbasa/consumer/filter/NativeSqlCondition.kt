package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement
import java.text.MessageFormat

internal class NativeSqlCondition<Meta : Any>(
    private val sqlPattern: String,
    private val fieldNames: List<String>
) : Condition<Meta>() {

    private lateinit var names: Array<String>

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::names.isInitialized) {
            names = Array(fieldNames.size) {
                val fieldName = fieldNames[it]
                val field = findField(fieldName)
                field.dbColumnName
            }
        }

        // make a replacement
        return MessageFormat.format(sqlPattern, *names)
    }

    override fun internalFillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
