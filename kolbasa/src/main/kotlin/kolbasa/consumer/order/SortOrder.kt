package kolbasa.consumer.order

/**
 * Defines the sort direction and null handling behavior for results ordering.
 *
 * SortOrder is used with [Order] to specify how messages should be sorted when consuming
 * from a queue. Internally, it maps directly to PostgreSQL `ORDER BY` clause modifiers.
 *
 * ## Available Options
 *
 * | Value              | SQL Clause           | Description                                      |
 * |--------------------|----------------------|--------------------------------------------------|
 * | [ASC]              | `asc`                | Ascending order, nulls position is DB-dependent  |
 * | [DESC]             | `desc`               | Descending order, nulls position is DB-dependent |
 * | [ASC_NULLS_FIRST]  | `asc nulls first`    | Ascending order, nulls appear first              |
 * | [DESC_NULLS_FIRST] | `desc nulls first`   | Descending order, nulls appear first             |
 * | [ASC_NULLS_LAST]   | `asc nulls last`     | Ascending order, nulls appear last               |
 * | [DESC_NULLS_LAST]  | `desc nulls last`    | Descending order, nulls appear last              |
 *
 * ## Usage Example
 *
 * ```kotlin
 * // Using with Order.by() factory method
 * val order = Order.by(USER_ID, SortOrder.DESC_NULLS_LAST)
 *
 * // Or use the more idiomatic extension methods on MetaField
 * val order = USER_ID.descNullsLast()
 * ```
 *
 * @see Order
 */
enum class SortOrder(internal val sql: String) {
    /** Smallest value first. Where nulls go is decided by PostgreSQL: with `asc` they come last. */
    ASC("asc"),

    /** Largest value first. Where nulls go is decided by PostgreSQL: with `desc` they come first. */
    DESC("desc"),

    /** Smallest value first, nulls before all of them. */
    ASC_NULLS_FIRST("asc nulls first"),

    /** Largest value first, nulls before all of them. */
    DESC_NULLS_FIRST("desc nulls first"),

    /** Smallest value first, nulls after all of them. */
    ASC_NULLS_LAST("asc nulls last"),

    /** Largest value first, nulls after all of them. */
    DESC_NULLS_LAST("desc nulls last")
}
