package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class IsNotNullCondition(private val field: MetaField<*>) : Condition() {

    override fun internalToSqlClause(queue: Queue<*>): String {
        return "${field.dbColumnName} is not null"
    }

    override fun internalFillPreparedQuery(
        queue: Queue<*>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    ) {
        // NOP
    }

}
