package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class OneOfCondition<T>(
    private val field: MetaField<T>,
    private val values: Collection<T>
) : Condition() {

    private val sqlClause = "${field.dbColumnName} = ANY (?)"

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValues(preparedStatement, columnIndex.nextIndex(), values)
    }

}
