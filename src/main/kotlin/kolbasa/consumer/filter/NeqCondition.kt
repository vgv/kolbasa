package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class NeqCondition<T>(val field: MetaField<T>, val value: T) : Condition() {

    private val sqlClause = "${field.dbColumnName}<>?"

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value)
    }

}
