package kolbasa.queue.meta

internal object MetaHelpers {

    fun defineIndexType(searchable: FieldOption): MetaIndexType {
        return when (searchable) {
            FieldOption.UNTOUCHED_UNIQUE  -> {
                MetaIndexType.UNTOUCHED_UNIQUE_INDEX
            }

            FieldOption.ALL_LIVE_UNIQUE -> {
                MetaIndexType.ALL_LIVE_UNIQUE_INDEX
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
    ALL_LIVE_UNIQUE_INDEX,
    UNTOUCHED_UNIQUE_INDEX
}

