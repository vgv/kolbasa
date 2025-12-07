package kolbasa.queue.meta

internal object MetaHelpers {

    fun defineIndexType(searchable: FieldOption): MetaIndexType {
        return when (searchable) {
            FieldOption.PENDING_ONLY_UNIQUE -> {
                MetaIndexType.PENDING_UNIQUE_INDEX
            }

            FieldOption.STRICT_UNIQUE -> {
                MetaIndexType.STRICT_UNIQUE_INDEX
            }

            FieldOption.SEARCH -> {
                MetaIndexType.JUST_INDEX
            }

            FieldOption.NONE -> {
                MetaIndexType.NO_INDEX
            }
        }
    }

}

internal enum class MetaIndexType {
    NO_INDEX,
    JUST_INDEX,
    STRICT_UNIQUE_INDEX,
    PENDING_UNIQUE_INDEX
}

