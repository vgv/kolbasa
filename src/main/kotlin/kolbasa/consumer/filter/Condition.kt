package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

abstract class Condition {

    /**
     * PostgreSQL `and` operator.
     *
     * Kotlin
     * ```kotlin
     * (USER_ID eq 42) and (USER_NAME like "abc%")
     * ```
     *
     * Java
     * ```java
     * var condition = eq(USER_ID, 42).and(like(USER_NAME, "abc%"));
     * ```
     *
     * means `(meta_user_id = 42) and (meta_user_name like 'abc%')`
     *
     * Chains are flattened, so `a and b and c` renders as one `and` clause rather than nested ones.
     */
    infix fun and(condition: Condition): Condition {
        return AndCondition(this, condition)
    }

    /**
     * PostgreSQL `or` operator.
     *
     * Kotlin
     * ```kotlin
     * (USER_ID eq 42) or (USER_ID eq 78)
     * ```
     *
     * Java
     * ```java
     * var condition = eq(USER_ID, 42).or(eq(USER_ID, 78));
     * ```
     *
     * means `(meta_user_id = 42) or (meta_user_id = 78)`
     *
     * Chains are flattened, so `a or b or c` renders as one `or` clause rather than nested ones.
     */
    infix fun or(condition: Condition): Condition {
        return OrCondition(this, condition)
    }

    /**
     * PostgreSQL `not` operator.
     *
     * Negates this condition. Kotlin callers can use the `!` prefix form, Java callers call it directly.
     *
     * Kotlin
     * ```kotlin
     * !(USER_ID eq 42)
     * ```
     *
     * Java
     * ```java
     * var condition = eq(USER_ID, 42).not();
     * ```
     *
     * means `not (meta_user_id = 42)`
     */
    operator fun not(): Condition {
        return NotCondition(this)
    }

    internal abstract fun toSqlClause(): String
    internal abstract fun fillPreparedQuery(preparedStatement: PreparedStatement, columnIndex: ColumnIndex)

}

