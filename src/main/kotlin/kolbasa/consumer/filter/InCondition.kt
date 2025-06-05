package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class InCondition<Meta : Any, T>(
    private val fieldName: String,
    private val values: Collection<T>
) : Condition<Meta>() {

    private lateinit var field: MetaField<Meta>

    override fun internalToSqlClause(queue: Queue<*, Meta>): String {
        if (!::field.isInitialized) {
            field = findField(fieldName)
        }

        return "${field.dbColumnName} = ANY (?)"
    }

    override fun internalFillPreparedQuery(
        queue: Queue<*, Meta>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    ) {
        field.fillPreparedStatementForValues(preparedStatement, columnIndex.nextIndex(), values)
    }

}
