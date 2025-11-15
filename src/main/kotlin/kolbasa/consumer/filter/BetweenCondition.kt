package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class BetweenCondition<T>(
    private val field: MetaField<T>,
    private val value: Pair<T, T>
) : Condition() {

    override fun internalToSqlClause(queue: Queue<*>): String {
        return "${field.dbColumnName} between ? and ?"
    }

    override fun internalFillPreparedQuery(queue: Queue<*>, preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value.first)
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value.second)
    }

}
