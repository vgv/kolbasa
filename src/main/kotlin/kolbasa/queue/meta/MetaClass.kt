package kolbasa.queue.meta

internal abstract class MetaClass<M : Any> {

    abstract val fields: List<MetaField<M>>

    abstract fun findMetaFieldByName(fieldName: String): MetaField<M>?

    abstract fun createInstance(values: Array<Any?>): M?

    companion object {
        fun <M : Any> of(metadata: Class<M>): MetaClass<M> {
            return if (metadata.kotlin.isData) {
                KotlinMetaClass(metadata.kotlin)
            } else if (metadata.isRecord) {
                JavaRecordMetaClass(metadata)
            } else {
                JavaBeanMetaClass(metadata)
            }
        }
    }
}

