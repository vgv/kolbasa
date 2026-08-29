package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

internal class BetweenCondition<T>(
    private val field: MetaField<T>,
    private val from: T,
    private val to: T,
) : Condition() {

    override fun toSqlClause(): String {
        return "${field.dbColumnName} between ? and ?"
    }

    override fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex) {
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), from)
        field.fillPreparedStatementForValue(preparedStatement, columnIndex.nextIndex(), to)
    }
}

/**
 * This class must be public because it provides the [Filter.between] infix method, but it's not intended for
 * direct use by library users. That's why all members of this class are marked by @JvmSynthetic. This makes
 * this class "empty" for Java users. Kotlin users, on the other hand, can't even create an instance of this class
 * because it has an "internal" constructor. So, it's useless for both worlds.
 */
class BetweenBuilder<T> internal constructor(
    private val field: MetaField<T>,
    private val from: T
) {
    /**
     * Completes a `between` condition by adding its upper bound.
     *
     * This is the second half of the Kotlin form `field between from and to`, which means SQL `between from and to`
     * – both bounds included. Java callers do not see this method and use [Filter.between] instead.
     *
     * @param to the upper bound, included
     */
    @JvmSynthetic
    infix fun and(to: T): Condition = BetweenCondition(field, from, to)
}
