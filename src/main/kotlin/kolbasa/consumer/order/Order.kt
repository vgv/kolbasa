package kolbasa.consumer.order

import kolbasa.queue.meta.MetaField

data class Order internal constructor(val field: MetaField<*>, val order: SortOrder) {

    // SQL 'order by' clause (column name + sort), like 'meta_column asc'
    internal val dbOrderClause = "${field.dbColumnName} ${order.sql}"

    companion object {

        /**
         * Helper DSL method to concatenate several [orders][Order] instead of direct
         * list manipulation.
         *
         * Example:
         * ```
         * ReceiveOptions(
         *    order = Meta::userId.asc() then Meta::saleId.ascNullsFirst() then Meta::priority.desc()
         * )
         * ```
         */
        infix fun List<Order>.then(next: List<Order>): List<Order> {
            return this + next
        }

        // -------------------------------------------------------------------------------------------
        fun of(field: MetaField<*>, order: SortOrder): Order {
            return Order(field, order)
        }

        // -------------------------------------------------------------------------------------------

        fun MetaField<*>.asc(): List<Order> {
            return listOf(of(this, SortOrder.ASC))
        }

        // -------------------------------------------------------------------------------------------

        fun MetaField<*>.desc(): List<Order> {
            return listOf(of(this, SortOrder.DESC))
        }

        // -------------------------------------------------------------------------------------------

        fun MetaField<*>.ascNullsFirst(): List<Order> {
            return listOf(of(this, SortOrder.ASC_NULLS_FIRST))
        }

        // -------------------------------------------------------------------------------------------

        fun MetaField<*>.descNullsFirst(): List<Order> {
            return listOf(of(this, SortOrder.DESC_NULLS_FIRST))
        }

        // -------------------------------------------------------------------------------------------

        fun MetaField<*>.ascNullsLast(): List<Order> {
            return listOf(of(this, SortOrder.ASC_NULLS_LAST))
        }

        // -------------------------------------------------------------------------------------------

        fun MetaField<*>.descNullsLast(): List<Order> {
            return listOf(of(this, SortOrder.DESC_NULLS_LAST))
        }
    }
}
