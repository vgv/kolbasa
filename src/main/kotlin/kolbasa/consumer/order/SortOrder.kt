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
 * // Using with Order.of() factory method
 * val order = Order.of(USER_ID, SortOrder.DESC_NULLS_LAST)
 *
 * // Or use the more idiomatic extension methods on MetaField
 * val order = USER_ID.descNullsLast()
 * ```
 *
 * @see Order
 */
enum class SortOrder(internal val sql: String) {
    ASC("asc"),
    DESC("desc"),
    ASC_NULLS_FIRST("asc nulls first"),
    DESC_NULLS_FIRST("desc nulls first"),
    ASC_NULLS_LAST("asc nulls last"),
    DESC_NULLS_LAST("desc nulls last")
}
