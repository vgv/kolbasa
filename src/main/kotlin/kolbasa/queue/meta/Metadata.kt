package kolbasa.queue.meta

data class Metadata(val fields: List<MetaField<*>>) {

    private val nameToFields = fields.associateBy { it.name }

    internal fun findMetaFieldByName(fieldName: String): MetaField<*>? = nameToFields[fieldName]

    companion object {
        val EMPTY = of(emptyList())

        fun of(vararg fields: MetaField<*>) = of(fields.toList())
        fun of(fields: List<MetaField<*>>) = Metadata(fields)
    }
}


