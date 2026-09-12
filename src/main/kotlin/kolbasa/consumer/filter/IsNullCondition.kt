package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class IsNullCondition(private val field: MetaField<*>) : Condition() {

    private val sqlClause = "${field.dbColumnName} is null"

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
