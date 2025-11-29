package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal abstract class AbstractOneValueCondition<T>(
    private val field: MetaField<T>,
    private val value: T
) : Condition() {

    abstract val operator: String

    override fun toSqlClause(): String {
        // Field Operator Parameter, like field=?, field>? etc.
        return "${field.dbColumnName} $operator ?"
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), value)
    }

}
