package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement
import java.text.MessageFormat

internal class NativeSqlCondition(
    private val sqlPattern: String,
    private val fields: Array<out MetaField<*>>
) : Condition() {

    private val names = Array(fields.size) {
        fields[it].dbColumnName
    }

    override fun internalToSqlClause(queue: Queue<*>): String {
        // make a replacement
        return MessageFormat.format(sqlPattern, *names)
    }

    override fun internalFillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
