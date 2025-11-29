package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class IsNotNullCondition(private val field: MetaField<*>) : Condition() {

    override fun toSqlClause(): String {
        return "${field.dbColumnName} is not null"
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
