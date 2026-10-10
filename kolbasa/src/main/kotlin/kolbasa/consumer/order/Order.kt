package kolbasa.consumer.order

import kolbasa.queue.meta.MetaField

/**
 * A complete `order by` specification for a `receive()` call.
 *
 * An Order holds one or more sort clauses, applied left to right, exactly like the columns
 * of a SQL `order by`. Build one with the factory methods below and combine with [then].
 *
 * ## Creating orders
 *
 * ```kotlin
 * // Single field
 * val byPriority = PRIORITY.desc()
 *
 * // Multiple fields
 * val complex = PRIORITY.desc() then CREATED_AT.asc() then USER_ID.ascNullsLast()
 * ```
 *
 * From Java:
 * ```java
 * var byPriority = Order.desc(PRIORITY);
 * var complex = Order.desc(PRIORITY).then(Order.asc(CREATED_AT));
 * var same    = Order.by(Order.desc(PRIORITY), Order.asc(CREATED_AT));
 * ```
 *
 * ## Available factories
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
 * @see SortOrder
 * @see kolbasa.consumer.ReceiveOptions
 * @see MetaField
 */
data class Order internal constructor(internal val clauses: List<OrderClause>) {

    /**
     * Appends [next] after this ordering.
     *
     * The same semantics as a second column in SQL `order by`: [next] only breaks ties
     * left unresolved by this ordering.
     *
     * ```kotlin
     * ReceiveOptions(
     *    order = USER_ID.asc() then SALE_ID.ascNullsFirst() then PRIORITY.desc()
     * )
     * ```
     */
    infix fun then(next: Order): Order {
        return Order(this.clauses + next.clauses)
    }

    companion object {

        /**
         * An ordering with no clauses, and the default value of
         * [ReceiveOptions.order][kolbasa.consumer.ReceiveOptions.order].
         *
         * This does **not** mean messages arrive in an arbitrary order. Kolbasa always appends its
         * own `scheduled_at` clause to every receive query, after any custom clauses. NONE
         * ordering therefore means "no custom ordering on top of the queue's natural order", which
         * is exactly what a `receive()` call does when you don't ask for anything else.
         *
         * Its main purpose is to be a neutral starting value when an ordering is assembled
         * conditionally. [then] on NONE returns the other ordering unchanged, so there are no null
         * checks and no special case for "nothing selected yet":
         *
         * ```kotlin
         * var order = Order.NONE
         * if (sortByPriority) order = order then PRIORITY.desc()
         * if (sortByAge)      order = order then CREATED_AT.asc()
         *
         * val messages = consumer.receive(queue, 10, ReceiveOptions(order = order))
         * ```
         *
         * The same from Java, where NONE is a plain static field:
         *
         *     var order = Order.NONE;
         *     if (sortByPriority) order = order.then(Order.desc(PRIORITY));
         *     if (sortByAge)      order = order.then(Order.asc(CREATED_AT));
         *
         *     var messages = consumer.receive(queue, 10, ReceiveOptions.builder().order(order).build());
         *
         * @see then
         * @see by
         */
        @JvmField
        val NONE: Order = Order(emptyList())

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
        @JvmStatic
        fun by(field: MetaField<*>, order: SortOrder): Order {
            val clause = OrderClause(field, order)
            return Order(listOf(clause))
        }

        /**
         * Combines several orderings into one, applied left to right.
         *
         * Equivalent to chaining [then], and the natural form when the ordering is built
         * dynamically or from Java:
         *
         * ```java
         * var byPriority = Order.desc(PRIORITY);
         * var byCreated = Order.asc(CREATED_AT);
         * var byAccountId = Order.ascNullsFirst(ACCOUNT_ID);

         * var order = Order.by(byPriority, byCreated, byAccountId);
         * ```
         *
         * An empty argument list produces an empty ordering, which leaves the default
         * queue ordering untouched.
         */
        @JvmStatic
        fun by(vararg orders: Order): Order {
            return Order(orders.flatMap { it.clauses })
        }

        /**
         * Same as [by], for an ordering already collected into a list or set.
         */
        @JvmStatic
        fun by(orders: Collection<Order>): Order {
            return Order(orders.flatMap { it.clauses })
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
        @JvmStatic
        fun MetaField<*>.asc(): Order {
            return by(this, SortOrder.ASC)
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
        @JvmStatic
        fun MetaField<*>.desc(): Order {
            return by(this, SortOrder.DESC)
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
        @JvmStatic
        fun MetaField<*>.ascNullsFirst(): Order {
            return by(this, SortOrder.ASC_NULLS_FIRST)
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
        @JvmStatic
        fun MetaField<*>.descNullsFirst(): Order {
            return by(this, SortOrder.DESC_NULLS_FIRST)
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
        @JvmStatic
        fun MetaField<*>.ascNullsLast(): Order {
            return by(this, SortOrder.ASC_NULLS_LAST)
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
        @JvmStatic
        fun MetaField<*>.descNullsLast(): Order {
            return by(this, SortOrder.DESC_NULLS_LAST)
        }
    }
}
