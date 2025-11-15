package kolbasa.consumer.filter

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class InCondition<T>(
    private val field: MetaField<T>,
    private val values: Collection<T>
) : Condition() {

    override fun internalToSqlClause(queue: Queue<*>): String {
        return "${field.dbColumnName} = ANY (?)"
    }

    override fun internalFillPreparedQuery(
        queue: Queue<*>,
        preparedStatement: PreparedStatement,
        columnIndex: ColumnIndex
    ) {
        field.fillPreparedStatementForValues(preparedStatement, columnIndex.nextIndex(), values)
    }

}
