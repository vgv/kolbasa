package kolbasa.queue.meta

import kolbasa.queue.QueueHelpers

internal object MetaHelpers {

    private val META_COLUMN_REGEX = Regex("([a-z])([A-Z]+)")

    fun generateMetaColumnName(fieldName: String): String {
        // convert Java field into column name, like someField -> some_field
        val snakeCaseName = fieldName.replace(META_COLUMN_REGEX, "$1_$2").lowercase()
        // add 'meta_' prefix
        return QueueHelpers.generateDbMetaColumnName(snakeCaseName)
    }

    fun defineIndexType(searchable: FieldOption): MetaIndexType {
        return when (searchable) {
            FieldOption.RELAXED_UNIQUE_SEARCH -> {
                MetaIndexType.RELAXED_UNIQUE_INDEX
            }

            FieldOption.UNIQUE_SEARCHABLE -> {
                MetaIndexType.STRICT_UNIQUE_INDEX
            }

            FieldOption.SEARCHABLE -> {
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
    RELAXED_UNIQUE_INDEX
}

