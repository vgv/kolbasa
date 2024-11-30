package kolbasa.queue.meta

internal abstract class MetaClass<Meta : Any> {

    abstract val fields: List<MetaField<Meta>>

    abstract fun findMetaFieldByName(fieldName: String): MetaField<Meta>?

    abstract fun createInstance(values: Array<Any?>): Meta?

    companion object {

        fun <Meta : Any> of(metadata: Class<Meta>): MetaClass<Meta>? {
            return if (metadata.kotlin.isData) {
                KotlinMetaClass(metadata.kotlin)
            } else if (metadata.isRecord) {
                JavaRecordMetaClass(metadata)
            } else {
                null
            }
        }

    }
}

