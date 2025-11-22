package kolbasa.queue.meta

import kolbasa.schema.Const
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.staticFunctions
import kotlin.reflect.full.valueParameters

internal object MetaHelpers {

    private val META_COLUMN_REGEX = Regex("([a-z])([A-Z]+)")

    fun generateMetaColumnName(fieldName: String): String {
        // convert Java field into column name, like someField -> some_field
        val snakeCaseName = fieldName.replace(META_COLUMN_REGEX, "$1_$2").lowercase()
        // add 'meta_' prefix
        return Const.META_FIELD_NAME_PREFIX + snakeCaseName
    }

    fun defineIndexType(searchable: FieldOption): MetaIndexType {
        return when (searchable) {
            FieldOption.UNIQUE_SEARCHABLE -> {
                MetaIndexType.UNIQUE_INDEX
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
    UNIQUE_INDEX
}

