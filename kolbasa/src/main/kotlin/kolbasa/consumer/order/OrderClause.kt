package kolbasa.consumer.order

import kolbasa.queue.meta.MetaField

internal data class OrderClause(
    val field: MetaField<*>,
    val order: SortOrder
)
