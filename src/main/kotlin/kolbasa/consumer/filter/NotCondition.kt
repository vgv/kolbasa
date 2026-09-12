package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class NotCondition(val condition: Condition) : Condition() {

    private val sqlClause = "not (${condition.toSqlClause()})"

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        condition.fillPreparedQuery(preparedStatement, columnIndex)
    }

}
