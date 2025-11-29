package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class TestCondition(val expr: String) : Condition() {

    override fun toSqlClause(): String {
        return expr
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // nothing to do
    }
}
