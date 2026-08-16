package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField

/**
 * Conditions for filtering messages by meta-fields.
 *
 * Kotlin callers get an infix DSL that reads like SQL:
 * ```kotlin
 * (USER_ID eq 42) and (CREATED_AT between t1 and t2)
 * ```
 *
 * Java callers should static-import the members and use the flat forms to have clean, nice API
 * ```java
 * import static kolbasa.consumer.filter.Filter.*;
 *
 * var userCondition = eq(USER_ID, 42);
 * var createdCondition = between(CREATED_AT, t1, t2);
 *
 * var complexCondition = or(userCondition, createdCondition);
 * ```
 */
object Filter {

    /**
     * PostgreSQL equality (`=`) operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID eq 42
     * ```
     * means `meta_user_id = 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.eq(value: T): Condition {
        return EqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `not equal (<>)` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID neq 42
     * ```
     * means `meta_user_id <> 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.neq(value: T): Condition {
        return NeqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `greater than (>)` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID greater 42
     * ```
     * means `meta_user_id > 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.greater(value: T): Condition {
        return GreaterThanCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `greater than or equal to (>=)` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID greaterEq 42
     * ```
     * means `meta_user_id >= 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.greaterEq(value: T): Condition {
        return GreaterThanOrEqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `less than (<)` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID less 42
     * ```
     * means `meta_user_id < 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.less(value: T): Condition {
        return LessThanCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `less than or equal to (<=)` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID lessEq 42
     * ```
     * means `meta_user_id <= 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.lessEq(value: T): Condition {
        return LessThanOrEqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `between` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID between 10 and 20
     * ```
     * means `(meta_user_id between 10 and 20)`
     *
     * Both values are inclusive, the same as `between` in SQL.
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmSynthetic // to hide this method from Java users, they don't need this infix version
    infix fun <T> MetaField<T>.between(from: T): BetweenBuilder<T> = BetweenBuilder(this, from)

    /**
     * PostgreSQL `between` operator.
     *
     * Note: This method is primarily for Java users. Kotlin callers should prefer the infix form, which reads much closer to
     * SQL. Nothing wrong with calling this method from Kotlin, it's just a "nice DSL" vs "regular method call" matter.
     *
     * Usage is the same as in SQL:
     *
     * ```java
     * between(USER_ID, 10, 20);
     * ```
     *
     * means `(meta_user_id between 10 and 20)`
     *
     * Both values are inclusive, the same as `between` in SQL.
     *
     * USER_ID is just a meta-field, declared something like this
     * ```java
     * static final MetaField<Integer> USER_ID = MetaField.ofInt("user_id");
     * ```
     */
    @JvmStatic
    fun <T> between(field: MetaField<T>, from: T, to: T): Condition = BetweenCondition(field, from, to)

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `like` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_NAME like "abc%"
     * ```
     * means `meta_user_name like 'abc%'`
     *
     * USER_NAME is just a meta-field, declared something like this
     * ```
     * val USER_NAME = MetaField.ofString("user_name")
     * ```
     */
    @JvmStatic
    infix fun MetaField<String>.like(value: String): Condition {
        return LikeCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `is null` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * isNull(USER_ID)
     * ```
     * means `meta_user_id is null`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    fun <T> isNull(field: MetaField<T>): Condition {
        return IsNullCondition(field)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `is not null` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * isNotNull(USER_ID)
     * ```
     * means `meta_user_id is not null`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    fun <T> isNotNull(field: MetaField<T>): Condition {
        return IsNotNullCondition(field)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL `ANY` operator.
     *
     * Usage is the same as in SQL:
     * ```kotlin
     * USER_ID oneOf listOf(1,2,3,4,5)
     * ```
     * means `meta_user_id = ANY (ARRAY [1,2,3,4,5])`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = MetaField.ofInt("user_id")
     * ```
     */
    @JvmStatic
    infix fun <T> MetaField<T>.oneOf(values: Collection<T>): Condition {
        return OneOfCondition(this, values)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * If you need to use some SQL function/expression which doesn't have a corresponding method in this
     * class, you can use this method to create a condition with a custom SQL pattern.
     *
     * Usage
     * ```kotlin
     * nativeSql("sin({0}) > 0.7 and {1}*{1}>1000", FIELD_1, FIELD_2)
     * ```
     *
     * This expression will be converted into this SQL expression:
     * ```kotlin
     * sin(meta_field_1) > 0.7 and meta_field_2 * meta_field_2>1000
     * ```
     *
     * FIELD_1 and FIELD_2 are just a meta-fields, declared something like this
     * ```
     * val FIELD_1 = MetaField.ofDouble("field_1")
     * val FIELD_2 = MetaField.ofLong("field_2")
     * ```
     *
     * Pattern format rules are the same as in [java.text.MessageFormat].
     *
     * Use it with caution, because it's not type-safe.
     * You can easily make a mistake in the SQL pattern or even introduce a SQL injection vulnerability.
     */
    @JvmStatic
    fun nativeSql(sqlPattern: String, vararg fields: MetaField<*>): Condition {
        return NativeSqlCondition(sqlPattern, fields)
    }

}

