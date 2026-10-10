package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement
import java.text.MessageFormat

internal data class NativeSqlCondition(
    private val sqlPattern: String,
    private val fields: List<MetaField<*>>
) : Condition() {

    private val names = Array(fields.size) { fields[it].dbColumnName }

    // make a replacement
    private val sqlClause = MessageFormat.format(sqlPattern, *names)

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

}
