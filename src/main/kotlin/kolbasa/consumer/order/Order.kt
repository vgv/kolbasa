package kolbasa.consumer.order

import kolbasa.consumer.JavaField
import kolbasa.queue.meta.MetaHelpers
import kotlin.reflect.KProperty1

data class Order<Meta> internal constructor(val metaPropertyName: String, val order: SortOrder) {

    /**
     * SQL meta-class column name, like 'meta_column'
     */
    internal val dbColumnName = MetaHelpers.generateMetaColumnName(metaPropertyName)

    /**
     * SQL 'order by' clause (column name + sort), like 'meta_column asc'
     */
    internal val dbOrderClause = "$dbColumnName ${order.sql}"

    companion object {

        fun <Meta> asc(property: KProperty1<Meta, *>): Order<Meta> {
            return Order(property.name, SortOrder.ASC)
        }

        @JvmStatic
        fun <Meta> asc(field: JavaField<Meta, *>): Order<Meta> {
            return Order(field.name, SortOrder.ASC)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> desc(property: KProperty1<Meta, *>): Order<Meta> {
            return Order(property.name, SortOrder.DESC)
        }

        @JvmStatic
        fun <Meta> desc(field: JavaField<Meta, *>): Order<Meta> {
            return Order(field.name, SortOrder.DESC)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> ascNullsFirst(property: KProperty1<Meta, *>): Order<Meta> {
            return Order(property.name, SortOrder.ASC_NULLS_FIRST)
        }

        @JvmStatic
        fun <Meta> ascNullsFirst(field: JavaField<Meta, *>): Order<Meta> {
            return Order(field.name, SortOrder.ASC_NULLS_FIRST)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> descNullsFirst(property: KProperty1<Meta, *>): Order<Meta> {
            return Order(property.name, SortOrder.DESC_NULLS_FIRST)
        }

        @JvmStatic
        fun <Meta> descNullsFirst(field: JavaField<Meta, *>): Order<Meta> {
            return Order(field.name, SortOrder.DESC_NULLS_FIRST)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> ascNullsLast(property: KProperty1<Meta, *>): Order<Meta> {
            return Order(property.name, SortOrder.ASC_NULLS_LAST)
        }

        @JvmStatic
        fun <Meta> ascNullsLast(field: JavaField<Meta, *>): Order<Meta> {
            return Order(field.name, SortOrder.ASC_NULLS_LAST)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> descNullsLast(property: KProperty1<Meta, *>): Order<Meta> {
            return Order(property.name, SortOrder.DESC_NULLS_LAST)
        }

        @JvmStatic
        fun <Meta> descNullsLast(field: JavaField<Meta, *>): Order<Meta> {
            return Order(field.name, SortOrder.DESC_NULLS_LAST)
        }
    }
}
