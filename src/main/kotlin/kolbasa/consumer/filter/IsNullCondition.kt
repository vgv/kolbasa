package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class IsNullCondition(private val field: MetaField<*>) : Condition() {

    override fun toSqlClause(): String {
        return "${field.dbColumnName} is null"

    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
