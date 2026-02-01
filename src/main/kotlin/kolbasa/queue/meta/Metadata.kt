package kolbasa.queue.meta

/**
 * Metadata for a queue.
 *
 * Metadata is a set of fields that will be stored in the database along with the message. Each message has
 * its own metadata values. Metadata is not required and can be empty.
 * It's useful if you want to filter messages by some fields or sort by them.
 *
 * Should you use metadata or store everything in the message body?
 * Please read the documentation for [Queue.metadata][kolbasa.queue.Queue.metadata] field
 */
data class Metadata(val fields: List<MetaField<*>>) {

    private val nameToFields = fields.associateBy { it.name }

    fun findByName(fieldName: String): MetaField<*>? = nameToFields[fieldName]

    companion object {
        val EMPTY = of(emptyList())

        fun of(vararg fields: MetaField<*>) = of(fields.toList())
        fun of(fields: List<MetaField<*>>) = Metadata(fields)
    }
}


