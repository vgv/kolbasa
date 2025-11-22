package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class BetweenCondition<T>(
    private val field: MetaField<T>,
    private val value: Pair<T, T>
) : Condition() {

    override fun toSqlClause(): String {
        return "${field.dbColumnName} between ? and ?"
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value.first)
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value.second)
    }

}
