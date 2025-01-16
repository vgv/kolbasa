package kolbasa.consumer.filter

import kolbasa.consumer.JavaField
import kotlin.reflect.KFunction1
import kotlin.reflect.KProperty1

object Filter {

    /**
     * PostgreSQL normal equality operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field eq 42
     * ```
     * means `meta_field = 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.eq(value: T): Condition<Meta> {
        return EqCondition(this.name, value)
    }

    /**
     * PostgreSQL normal equality operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field eq 42
     * ```
     * means `meta_field = 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.eq(value: T): Condition<Meta> {
        return EqCondition(this.name, value)
    }

    /**
     * PostgreSQL normal equality operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.eq(JavaField.of(...), 42)
     * ```
     * means `meta_field = 42`
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.eq(value: T): Condition<Meta> {
        return EqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL normal 'not equal' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field neq 42
     * ```
     * means `meta_field <> 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.neq(value: T): Condition<Meta> {
        return NeqCondition(this.name, value)
    }

    /**
     * PostgreSQL normal 'not equal' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field neq 42
     * ```
     * means `meta_field <> 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.neq(value: T): Condition<Meta> {
        return NeqCondition(this.name, value)
    }

    /**
     * PostgreSQL normal 'not equal' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.neq(JavaField.of(...), 42)
     * ```
     * means `meta_field <> 42`
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.neq(value: T): Condition<Meta> {
        return NeqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'greater than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field greater 42
     * ```
     * means `meta_field > 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.greater(value: T): Condition<Meta> {
        return GreaterThanCondition(this.name, value)
    }

    /**
     * PostgreSQL 'greater than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field greater 42
     * ```
     * means `meta_field > 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.greater(value: T): Condition<Meta> {
        return GreaterThanCondition(this.name, value)
    }

    /**
     * PostgreSQL 'greater than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.greater(JavaField.of(...), 42)
     * ```
     * means `meta_field > 42`
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.greater(value: T): Condition<Meta> {
        return GreaterThanCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'greater than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field greaterEq 42
     * ```
     * means `meta_field >= 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.greaterEq(value: T): Condition<Meta> {
        return GreaterThanOrEqCondition(this.name, value)
    }

    /**
     * PostgreSQL 'greater than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field greaterEq 42
     * ```
     * means `meta_field >= 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.greaterEq(value: T): Condition<Meta> {
        return GreaterThanOrEqCondition(this.name, value)
    }

    /**
     * PostgreSQL 'greater than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.greaterEq(JavaField.of(...), 42)
     * ```
     * means `meta_field >= 42`
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.greaterEq(value: T): Condition<Meta> {
        return GreaterThanOrEqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'less than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field less 42
     * ```
     * means `meta_field < 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.less(value: T): Condition<Meta> {
        return LessThanCondition(this.name, value)
    }

    /**
     * PostgreSQL 'less than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field less 42
     * ```
     * means `meta_field < 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.less(value: T): Condition<Meta> {
        return LessThanCondition(this.name, value)
    }

    /**
     * PostgreSQL 'less than' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.less(JavaField.of(...), 42)
     * ```
     * means `meta_field < 42`
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.less(value: T): Condition<Meta> {
        return LessThanCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'less than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field lessEq 42
     * ```
     * means `meta_field <= 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.lessEq(value: T): Condition<Meta> {
        return LessThanOrEqCondition(this.name, value)
    }

    /**
     * PostgreSQL 'less than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field lessEq 42
     * ```
     * means `meta_field <= 42`
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.lessEq(value: T): Condition<Meta> {
        return LessThanOrEqCondition(this.name, value)
    }

    /**
     * PostgreSQL 'less than or equal to' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.lessEq(JavaField.of(...), 42)
     * ```
     * means `meta_field <= 42`
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.lessEq(value: T): Condition<Meta> {
        return LessThanOrEqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'between' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field between Pair(10, 20)
     * ```
     * means `(meta_field between 10 and 20)`
     *
     * Both values are inclusive, the same as `between` in SQL.
     */
    infix fun <Meta : Any, T : Comparable<T>> KProperty1<Meta, T?>.between(value: Pair<T, T>): Condition<Meta> {
        return BetweenCondition(this.name, value)
    }

    /**
     * PostgreSQL 'between' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field between Pair(10, 20)
     * ```
     * means `(meta_field between 10 and 20)`
     *
     * Both values are inclusive, the same as `between` in SQL.
     */
    infix fun <Meta : Any, T : Comparable<T>> KFunction1<Meta, T?>.between(value: Pair<T, T>): Condition<Meta> {
        return BetweenCondition(this.name, value)
    }

    /**
     * PostgreSQL 'between' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.between(JavaField.of(...), new Pair<>(10, 20))
     * ```
     * means `(meta_field between 10 and 20)`
     *
     * Both values are inclusive, the same as `between` in SQL.
     */
    @JvmStatic
    infix fun <Meta : Any, T : Comparable<T>> JavaField<Meta, T?>.between(value: Pair<T, T>): Condition<Meta> {
        return BetweenCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL classic like operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field like "abc%"
     * ```
     * means `meta_field like 'abc%'`
     */
    infix fun <Meta : Any> KProperty1<Meta, String?>.like(value: String): Condition<Meta> {
        return LikeCondition(this.name, value)
    }

    /**
     * PostgreSQL classic like operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field like "abc%"
     * ```
     * means `meta_field like 'abc%'`
     */
    infix fun <Meta : Any> KFunction1<Meta, String?>.like(value: String): Condition<Meta> {
        return LikeCondition(this.name, value)
    }

    /**
     * PostgreSQL classic like operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.like(JavaField.of(...), "abc%")
     * ```
     * means `meta_field like 'abc%'`
     */
    @JvmStatic
    infix fun <Meta : Any> JavaField<Meta, String?>.like(value: String): Condition<Meta> {
        return LikeCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <Meta : Any> Condition<Meta>.and(condition: Condition<Meta>): Condition<Meta> {
        return AndCondition(this, condition)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <Meta : Any> Condition<Meta>.or(condition: Condition<Meta>): Condition<Meta> {
        return OrCondition(this, condition)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'is null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * isNull(Meta::field)
     * ```
     * means `meta_field is null`
     */
    fun <Meta : Any, T : Comparable<T>> isNull(property: KProperty1<Meta, T?>): Condition<Meta> {
        return IsNullCondition(property.name)
    }

    /**
     * PostgreSQL 'is null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * isNull(Meta::field)
     * ```
     * means `meta_field is null`
     */
    fun <Meta : Any, T : Comparable<T>> isNull(property: KFunction1<Meta, T?>): Condition<Meta> {
        return IsNullCondition(property.name)
    }

    /**
     * PostgreSQL 'is null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.isNull(JavaField.of(...))
     * ```
     * means `meta_field is null`
     */
    @JvmStatic
    fun <Meta : Any, T : Comparable<T>> isNull(property: JavaField<Meta, T?>): Condition<Meta> {
        return IsNullCondition(property.name)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * PostgreSQL 'is not null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * isNotNull(Meta::field)
     * ```
     * means `meta_field is not null`
     */
    fun <Meta : Any, T : Comparable<T>> isNotNull(property: KProperty1<Meta, T?>): Condition<Meta> {
        return IsNotNullCondition(property.name)
    }

    /**
     * PostgreSQL 'is not null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * isNotNull(Meta::field)
     * ```
     * means `meta_field is not null`
     */
    fun <Meta : Any, T : Comparable<T>> isNotNull(property: KFunction1<Meta, T?>): Condition<Meta> {
        return IsNotNullCondition(property.name)
    }

    /**
     * PostgreSQL 'is not null' operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.isNotNull(JavaField.of(...))
     * ```
     * means `meta_field is not null`
     */
    @JvmStatic
    fun <Meta : Any, T : Comparable<T>> isNotNull(property: JavaField<Meta, T?>): Condition<Meta> {
        return IsNotNullCondition(property.name)
    }

    /**
     * PostgreSQL in operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field in listOf(42)
     * ```
     * means `meta_field = ANY (42)`
     */
    infix fun <Meta : Any, T> KProperty1<Meta, T?>.`in`(values: Collection<T>): Condition<Meta> {
        return InCondition(this.name, values)
    }

    /**
     * PostgreSQL in operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Meta::field in listOf(42)
     * ```
     * means `meta_field = ANY (42)`
     */
    infix fun <Meta : Any, T> KFunction1<Meta, T?>.`in`(values: Collection<T>): Condition<Meta> {
        return InCondition(this.name, values)
    }

    /**
     * PostgreSQL normal equality operator.
     *
     * Usage is the same as in SQL:
     * ```
     * Filter.eq(JavaField.of(...), ArrayList(42))
     * ```
     * means `meta_field = ANY (42)`
     */
    @JvmStatic
    infix fun <Meta : Any, T> JavaField<Meta, T?>.`in`(values: Collection<T>): Condition<Meta> {
        return InCondition(this.name, values)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    fun <Meta : Any> not(condition: Condition<Meta>): Condition<Meta> {
        return NotCondition(condition)
    }

    // -------------------------------------------------------------------------------------------
    /**
     * If you need to use some SQL function/expression which doesn't have a corresponding method in this
     * class, you can use this method to create a condition with a custom SQL pattern.
     *
     * Usage
     * ```
     * nativeSql("sin({0}) > 0.7 and {1}*{1}>1000", MyMeta::field1, MyMeta::field2)
     * ```
     *
     * This expression will be converted into this SQL expression:
     * ```
     * sin(meta_field1) > 0.7 and meta_field2 * meta_field2>1000
     * ```
     *
     * Pattern format rules are the same as in [java.text.MessageFormat].
     *
     * Use it with caution, because it's not type-safe.
     * You can easily make a mistake in the SQL pattern or even introduce a SQL injection vulnerability.
     */
    fun <Meta : Any> nativeSql(sqlPattern: String, vararg properties: KProperty1<Meta, *>): Condition<Meta> {
        return NativeSqlCondition(sqlPattern, properties.map { it.name })
    }

    /**
     * If you need to use some SQL function/expression which doesn't have a corresponding method in this
     * class, you can use this method to create a condition with a custom SQL pattern.
     *
     * Usage
     * ```
     * nativeSql("sin({0}) > 0.7 and {1}*{1}>1000", MyMeta::field1, MyMeta::field2)
     * ```
     *
     * This expression will be converted into this SQL expression:
     * ```
     * sin(meta_field1) > 0.7 and meta_field2 * meta_field2>1000
     * ```
     *
     * Pattern format rules are the same as in [java.text.MessageFormat].
     *
     * Use it with caution, because it's not type-safe.
     * You can easily make a mistake in the SQL pattern or even introduce a SQL injection vulnerability.
     */
    fun <Meta : Any> nativeSql(sqlPattern: String, vararg properties: KFunction1<Meta, *>): Condition<Meta> {
        return NativeSqlCondition(sqlPattern, properties.map { it.name })
    }

    /**
     * If you need to use some SQL function/expression which doesn't have a corresponding method in this
     * class, you can use this method to create a condition with a custom SQL pattern.
     *
     * Usage
     * ```
     * nativeSql("sin({0}) > 0.7 and {1}*{1}>1000", JavaField.of(...), JavaField.of(...))
     * ```
     *
     * This expression will be converted into this SQL expression:
     * ```
     * sin(meta_field1) > 0.7 and meta_field2 * meta_field2>1000
     * ```
     *
     * Pattern format rules are the same as in [java.text.MessageFormat].
     *
     * Use it with caution, because it's not type-safe.
     * You can easily make a mistake in the SQL pattern or even introduce a SQL injection vulnerability.
     */
    @JvmStatic
    fun <Meta : Any> nativeSql(sqlPattern: String, vararg properties: JavaField<Meta, *>): Condition<Meta> {
        return NativeSqlCondition(sqlPattern, properties.map { it.name })
    }

}

