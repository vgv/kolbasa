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

        fun <Meta> KProperty1<Meta, *>.asc(): Order<Meta> {
            return Order(name, SortOrder.ASC)
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.asc(): Order<Meta> {
            return Order(name, SortOrder.ASC)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.desc(): Order<Meta> {
            return Order(name, SortOrder.DESC)
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.desc(): Order<Meta> {
            return Order(name, SortOrder.DESC)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.ascNullsFirst(): Order<Meta> {
            return Order(name, SortOrder.ASC_NULLS_FIRST)
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.ascNullsFirst(): Order<Meta> {
            return Order(name, SortOrder.ASC_NULLS_FIRST)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.descNullsFirst(): Order<Meta> {
            return Order(name, SortOrder.DESC_NULLS_FIRST)
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.descNullsFirst(): Order<Meta> {
            return Order(name, SortOrder.DESC_NULLS_FIRST)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.ascNullsLast(): Order<Meta> {
            return Order(name, SortOrder.ASC_NULLS_LAST)
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.ascNullsLast(): Order<Meta> {
            return Order(name, SortOrder.ASC_NULLS_LAST)
        }

        // -------------------------------------------------------------------------------------------

        fun <Meta> KProperty1<Meta, *>.descNullsLast(): Order<Meta> {
            return Order(name, SortOrder.DESC_NULLS_LAST)
        }

        @JvmStatic
        fun <Meta> JavaField<Meta, *>.descNullsLast(): Order<Meta> {
            return Order(name, SortOrder.DESC_NULLS_LAST)
        }
    }
}
