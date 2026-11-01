package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal data class NativeSqlCondition(
    private val sqlPattern: String,
    private val fields: List<MetaField<*>>
) : Condition() {

    // Replace {0}, {1} and so on with the column names. Nothing else in the pattern is touched, so quotes, braces and
    // the rest go into SQL as written
    private val sqlClause = PLACEHOLDER.replace(sqlPattern) { placeholder: MatchResult ->
        val index = placeholder.groupValues[1].toIntOrNull()
        require(index != null && index < fields.size) {
            "Pattern '$sqlPattern' has ${placeholder.value}, but there are only ${fields.size} fields"
        }
        fields[index].dbColumnName
    }

    override fun toSqlClause() = sqlClause

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        // NOP
    }

    private companion object {
        val PLACEHOLDER = Regex("""\{(\d+)}""")
    }

}
