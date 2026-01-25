package kolbasa.consumer.order

import kolbasa.queue.meta.MetaField

/**
 * Represents an ordering specification for consuming messages from a queue.
 *
 * Order combines a [MetaField] with a [SortOrder] to define how messages should be
 * sorted when retrieved. Multiple orders can be combined to create complex sorting rules.
 *
 * ## Creating Orders
 *
 * The recommended way to create orders is using the extension methods on [MetaField]:
 *
 * ```kotlin
 * import kolbasa.consumer.order.Order.Companion.asc
 * import kolbasa.consumer.order.Order.Companion.desc
 * import kolbasa.consumer.order.Order.Companion.then
 *
 * // Single field ordering
 * val byPriority = PRIORITY.desc()
 *
 * // Multiple field ordering using 'then' infix function
 * val complexOrder = PRIORITY.desc() then CREATED_AT.asc() then USER_ID.ascNullsLast()
 * ```
 *
 * ## Available Extension Methods
 *
 * | Method              | Sort Direction | Nulls Handling    |
 * |---------------------|----------------|-------------------|
 * | [asc]               | Ascending      | DB-dependent      |
 * | [desc]              | Descending     | DB-dependent      |
 * | [ascNullsFirst]     | Ascending      | Nulls first       |
 * | [descNullsFirst]    | Descending     | Nulls first       |
 * | [ascNullsLast]      | Ascending      | Nulls last        |
 * | [descNullsLast]     | Descending     | Nulls last        |
 *
 * ## Usage with ReceiveOptions
 *
 * ```kotlin
 * val options = ReceiveOptions(
 *     order = PRIORITY.desc() then ACCOUNT_ID.asc()
 * )
 *
 * val messages = consumer.receive(queue, 10, options)
 * ```
 *
 * @property field the metadata field to sort by
 * @property order the sort direction
 *
 * @see SortOrder
 * @see kolbasa.consumer.ReceiveOptions
 * @see MetaField
 */
data class Order internal constructor(val field: MetaField<*>, val order: SortOrder) {

    // SQL 'order by' clause (column name + sort), like 'meta_column asc'
    internal val dbOrderClause = "${field.dbColumnName} ${order.sql}"

    companion object {

        /**
         * Helper DSL method to concatenate several [orders][Order] instead of direct
         * list manipulation.
         *
         * Example:
         * ```kotlin
         * ReceiveOptions(
         *    order = USER_ID.asc() then SALE_ID.ascNullsFirst() then PRIORITY.desc()
         * )
         * ```
         */
        infix fun List<Order>.then(next: List<Order>): List<Order> {
            return this + next
        }

        /**
         * Creates an [Order] from a [MetaField] and [SortOrder].
         *
         * This is a low-level factory method. For more idiomatic usage, prefer the extension
         * methods like [asc], [desc], [ascNullsFirst], etc.
         *
         * @param field the metadata field to sort by
         * @param order the sort direction and null handling
         * @return an Order instance
         */
        fun of(field: MetaField<*>, order: SortOrder): Order {
            return Order(field, order)
        }

        /**
         * Creates an ascending order for this field.
         *
         * Null handling behavior depends on the database default (in PostgreSQL, nulls are last for ASC).
         *
         * ```kotlin
         * val order = PRIORITY.asc()
         * ```
         *
         * @return a list containing a single ascending Order for this field
         * @see ascNullsFirst
         * @see ascNullsLast
         */
        fun MetaField<*>.asc(): List<Order> {
            return listOf(of(this, SortOrder.ASC))
        }

        /**
         * Creates a descending order for this field.
         *
         * Null handling behavior depends on the database default (in PostgreSQL, nulls are first for DESC).
         *
         * ```kotlin
         * val order = PRIORITY.desc()
         * ```
         *
         * @return a list containing a single descending Order for this field
         * @see descNullsFirst
         * @see descNullsLast
         */
        fun MetaField<*>.desc(): List<Order> {
            return listOf(of(this, SortOrder.DESC))
        }

        /**
         * Creates an ascending order for this field with nulls appearing first.
         *
         * ```kotlin
         * val order = PRIORITY.ascNullsFirst()
         * ```
         *
         * @return a list containing a single ascending Order with nulls first
         */
        fun MetaField<*>.ascNullsFirst(): List<Order> {
            return listOf(of(this, SortOrder.ASC_NULLS_FIRST))
        }

        /**
         * Creates a descending order for this field with nulls appearing first.
         *
         * ```kotlin
         * val order = PRIORITY.descNullsFirst()
         * ```
         *
         * @return a list containing a single descending Order with nulls first
         */
        fun MetaField<*>.descNullsFirst(): List<Order> {
            return listOf(of(this, SortOrder.DESC_NULLS_FIRST))
        }

        /**
         * Creates an ascending order for this field with nulls appearing last.
         *
         * ```kotlin
         * val order = PRIORITY.ascNullsLast()
         * ```
         *
         * @return a list containing a single ascending Order with nulls last
         */
        fun MetaField<*>.ascNullsLast(): List<Order> {
            return listOf(of(this, SortOrder.ASC_NULLS_LAST))
        }

        /**
         * Creates a descending order for this field with nulls appearing last.
         *
         * ```kotlin
         * val order = PRIORITY.descNullsLast()
         * ```
         *
         * @return a list containing a single descending Order with nulls last
         */
        fun MetaField<*>.descNullsLast(): List<Order> {
            return listOf(of(this, SortOrder.DESC_NULLS_LAST))
        }
    }
}
