package kolbasa.consumer.filter

import kolbasa.utils.ColumnIndex
import java.sql.PreparedStatement

/**
 * One filter condition – the `where` part of a receive, mutate or count call.
 *
 * A condition compares a meta field with a value, for example `meta_user_id = 42`. You do not create conditions
 * directly: build them with the methods of [Filter], then join them with [and], [or] and [not]. Kolbasa turns the
 * result into a single SQL `where` clause and sends your values as query parameters, so they are never pasted into
 * the SQL text.
 *
 * ## Usage Example
 *
 * ```kotlin
 * import kolbasa.consumer.filter.Filter.eq
 * import kolbasa.consumer.filter.Filter.like
 *
 * val condition = (USER_ID eq 42) and (USER_NAME like "abc%")
 *
 * val messages = consumer.receive(queue, 10, ReceiveOptions(filter = condition))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var condition = Filter.eq(USER_ID, 42).and(Filter.like(USER_NAME, "abc%"));
 *
 * var messages = consumer.receive(queue, 10, ReceiveOptions.builder().filter(condition).build());
 * ```
 *
 * A condition is immutable and holds no connection, so you can build one once and reuse it in as many calls as you
 * like. [and], [or] and [not] never change the conditions they are called on, they return a new one.
 *
 * Do not write your own subclass of this class. If you need a condition Kolbasa cannot express, use [Filter.nativeSql].
 *
 * @see Filter
 */
sealed class Condition {

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

