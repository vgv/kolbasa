package kolbasa.consumer.order

import kolbasa.consumer.JavaField
import kolbasa.queue.meta.MetaHelpers
import kotlin.reflect.KProperty1

data class Order<Meta> internal constructor(val metaPropertyName: String, val order: SortOrder) {

    // SQL meta-class column name, like 'meta_column'
    internal val dbColumnName = MetaHelpers.generateMetaColumnName(metaPropertyName)

    // SQL 'order by' clause (column name + sort), like 'meta_column asc'
    internal val dbOrderClause = "$dbColumnName ${order.sql}"

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
        infix fun <Meta> List<Order<Meta>>.then(next: List<Order<Meta>>): List<Order<Meta>> {
            return this + next
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.asc(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.ASC))
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.asc(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.ASC))
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.desc(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.DESC))
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.desc(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.DESC))
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.ascNullsFirst(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.ASC_NULLS_FIRST))
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.ascNullsFirst(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.ASC_NULLS_FIRST))
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.descNullsFirst(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.DESC_NULLS_FIRST))
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.descNullsFirst(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.DESC_NULLS_FIRST))
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.ascNullsLast(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.ASC_NULLS_LAST))
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.ascNullsLast(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.ASC_NULLS_LAST))
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.descNullsLast(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.DESC_NULLS_LAST))
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.descNullsLast(): List<Order<Meta>> {
            return listOf(Order(name, SortOrder.DESC_NULLS_LAST))
        }
    }
}
