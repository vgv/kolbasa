package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField

object Filter {

    /**
     * PostgreSQL normal equality operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID eq 42
     * ```
     * means `meta_user_id = 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.eq(value: T): Condition {
        return EqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL normal 'not equal' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID neq 42
     * ```
     * means `meta_user_id <> 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.neq(value: T): Condition {
        return NeqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'greater than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID greater 42
     * ```
     * means `meta_user_id > 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.greater(value: T): Condition {
        return GreaterThanCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'greater than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID greaterEq 42
     * ```
     * means `meta_user_id >= 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.greaterEq(value: T): Condition {
        return GreaterThanOrEqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'less than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID less 42
     * ```
     * means `meta_user_id < 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.less(value: T): Condition {
        return LessThanCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'less than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID lessEq 42
     * ```
     * means `meta_user_id <= 42`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.lessEq(value: T): Condition {
        return LessThanOrEqCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'between' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID between Pair(10, 20)
     * ```
     * means `(meta_user_id between 10 and 20)`
     *
     * Both values are inclusive, the same as `between` in SQL.
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.between(value: Pair<T, T>): Condition {
        return BetweenCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL classic like operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_NAME like "abc%"
     * ```
     * means `meta_user_name like 'abc%'`
     *
     * USER_NAME is just a meta-field, declared something like this
     * ```
     * val USER_NAME = StringField("user_name")
     * ```
     */
    infix fun MetaField<String>.like(value: String): Condition {
        return LikeCondition(this, value)
    }

    // -------------------------------------------------------------------------------------------

    @JvmStatic
    infix fun Condition.and(condition: Condition): Condition {
        return AndCondition(this, condition)
    }

    // -------------------------------------------------------------------------------------------

    @JvmStatic
    infix fun Condition.or(condition: Condition): Condition {
        return OrCondition(this, condition)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'is null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * isNull(USER_ID)
     * ```
     * means `meta_user_id is null`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    fun <T> isNull(field: MetaField<T>): Condition {
        return IsNullCondition(field)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL 'is not null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * isNotNull(USER_ID)
     * ```
     * means `meta_user_id is not null`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    fun <T> isNotNull(field: MetaField<T>): Condition {
        return IsNotNullCondition(field)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * PostgreSQL in operator.
     *
     * Usage is the same as in SQL:
     * ```
     * USER_ID in listOf(42)
     * ```
     * means `meta_user_id = ANY (42)`
     *
     * USER_ID is just a meta-field, declared something like this
     * ```
     * val USER_ID = IntField("user_id")
     * ```
     */
    infix fun <T> MetaField<T>.`in`(values: Collection<T>): Condition {
        return InCondition(this, values)
    }

    // -------------------------------------------------------------------------------------------

    @JvmStatic
    fun not(condition: Condition): Condition {
        return NotCondition(condition)
    }

    // -------------------------------------------------------------------------------------------

    /**
     * If you need to use some SQL function/expression which doesn't have a corresponding method in this
     * class, you can use this method to create a condition with a custom SQL pattern.
     *
     * Usage
     * ```
     * nativeSql("sin({0}) > 0.7 and {1}*{1}>1000", FIELD_1, FIELD_2)
     * ```
     *
     * This expression will be converted into this SQL expression:
     * ```
     * sin(meta_field_1) > 0.7 and meta_field_2 * meta_field_2>1000
     * ```
     *
     * FIELD_1 and FIELD_2 are just a meta-fields, declared something like this
     * ```
     * val FIELD_1 = DoubleField("field_1")
     * val FIELD_2 = LongField("field_2")
     * ```
     *
     * Pattern format rules are the same as in [java.text.MessageFormat].
     *
     * Use it with caution, because it's not type-safe.
     * You can easily make a mistake in the SQL pattern or even introduce a SQL injection vulnerability.
     */
    fun nativeSql(sqlPattern: String, vararg fields: MetaField<*>): Condition {
        return NativeSqlCondition(sqlPattern, fields)
    }

}

