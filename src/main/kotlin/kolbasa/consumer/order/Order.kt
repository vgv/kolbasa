package kolbasa.consumer.order

import kolbasa.consumer.JavaField
import kolbasa.queue.meta.MetaHelpers
import kotlin.reflect.KProperty1

data class Order<M> internal constructor(val metaPropertyName: String, val order: SortOrder) {

    /**
     * SQL 'order by' clause (column name + sort), like 'some_column asc'
     */
    internal val dbOrderClause = "${MetaHelpers.generateMetaColumnName(metaPropertyName)} ${order.sql}"

    companion object {

        fun <M> asc(property: KProperty1<M, *>): Order<M> {
            return Order(property.name, SortOrder.ASC)
        }

        @JvmStatic
        fun <M> asc(field: JavaField<M, *>): Order<M> {
            return Order(field.name, SortOrder.ASC)
        }

        // -------------------------------------------------------------------------------------------

        fun <M> desc(property: KProperty1<M, *>): Order<M> {
            return Order(property.name, SortOrder.DESC)
        }

        @JvmStatic
        fun <M> desc(field: JavaField<M, *>): Order<M> {
            return Order(field.name, SortOrder.DESC)
        }

        // -------------------------------------------------------------------------------------------

        fun <M> ascNullsFirst(property: KProperty1<M, *>): Order<M> {
            return Order(property.name, SortOrder.ASC_NULLS_FIRST)
        }

        @JvmStatic
        fun <M> ascNullsFirst(field: JavaField<M, *>): Order<M> {
            return Order(field.name, SortOrder.ASC_NULLS_FIRST)
        }

        // -------------------------------------------------------------------------------------------

        fun <M> descNullsFirst(property: KProperty1<M, *>): Order<M> {
            return Order(property.name, SortOrder.DESC_NULLS_FIRST)
        }

        @JvmStatic
        fun <M> descNullsFirst(field: JavaField<M, *>): Order<M> {
            return Order(field.name, SortOrder.DESC_NULLS_FIRST)
        }

        // -------------------------------------------------------------------------------------------

        fun <M> ascNullsLast(property: KProperty1<M, *>): Order<M> {
            return Order(property.name, SortOrder.ASC_NULLS_LAST)
        }

        @JvmStatic
        fun <M> ascNullsLast(field: JavaField<M, *>): Order<M> {
            return Order(field.name, SortOrder.ASC_NULLS_LAST)
        }

        // -------------------------------------------------------------------------------------------

        fun <M> descNullsLast(property: KProperty1<M, *>): Order<M> {
            return Order(property.name, SortOrder.DESC_NULLS_LAST)
        }

        @JvmStatic
        fun <M> descNullsLast(field: JavaField<M, *>): Order<M> {
            return Order(field.name, SortOrder.DESC_NULLS_LAST)
        }
    }
}
