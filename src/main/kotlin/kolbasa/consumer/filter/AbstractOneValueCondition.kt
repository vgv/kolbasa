package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal abstract class AbstractOneValueCondition<T>(
    private val field: MetaField<T>,
    private val value: T
) : Condition() {

    abstract val operator: String

    override fun internalToSqlClause(queue: Queue<*>): String {
        // Field Operator Parameter, like field=?, field>? etc.
        return "${field.dbColumnName} $operator ?"
    }

    override fun internalFillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value)
    }

}
