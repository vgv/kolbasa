package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class IsNullCondition<Meta : Any>(private val fieldName: String) : Condition<Meta>() {

    private lateinit var field: MetaField<Meta>

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::field.isInitialized) {
            field = findField(fieldName)
        }

        return "${field.dbColumnName} is null"

    }

    override fun internalFillPreparedQuery(
        queue: Queue<*, Meta>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    ) {
        // NOP
    }

}
