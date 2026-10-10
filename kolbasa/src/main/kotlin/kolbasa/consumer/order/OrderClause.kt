package kolbasa.consumer.order

import kolbasa.queue.meta.MetaField

internal data class OrderClause(
    val field: MetaField<*>,
    val order: SortOrder
) {

    // SQL 'order by' clause (column name + sort), like 'meta_column asc'
    internal val dbOrderClause = "${field.dbColumnName} ${order.sql}"

}
