package kolbasa.consumer.order

enum class SortOrder(val sql: String) {
    ASC("asc"),
    DESC("desc"),
    ASC_NULLS_FIRST("asc nulls first"),
    DESC_NULLS_FIRST("desc nulls first"),
    ASC_NULLS_LAST("asc nulls last"),
    DESC_NULLS_LAST("desc nulls last")
}
