package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import java.sql.PreparedStatement

internal class BetweenCondition<Meta : Any, T>(
    private val fieldName: String,
    private val value: Pair<T, T>
) : Condition<Meta>() {

    private lateinit var field: MetaField<Meta>

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::field.isInitialized) {
            field = findField(fieldName)
        }

        return "${field.dbColumnName} between ? and ?"
    }

    override fun internalFillPreparedQuery(queue: Queue<*, Meta>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value.first)
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value.second)
    }

}
