package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class NotCondition(val condition: Condition) : Condition() {

    override fun toSqlClause(): String {
        return "not (${condition.toSqlClause()})"
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        condition.fillPreparedQuery(preparedStatement, columnIndex)
    }

}
