package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

abstract class Condition {

    internal abstract fun toSqlClause(): String

    internal abstract fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex)

}

