package kolbasa.queue.meta

import java.lang.reflect.Constructor

internal class JavaRecordMetaClass<M : Any>(recordClass: Class<M>) : MetaClass<M>() {

    override val fields: List<JavaRecordPropertyMetaField<M>>

    private val recordConstructor: Constructor<M>
    private val propertiesToFields: Map<String, JavaRecordPropertyMetaField<M>>

    init {
        check(recordClass.isRecord) {
            "Class $recordClass must be a Java record class"
        }

        recordConstructor = MetaHelpers.findCanonicalRecordConstructor(recordClass)

        val tempFields = mutableListOf<JavaRecordPropertyMetaField<M>>()
        val tempPropertiesToFields = mutableMapOf<String, JavaRecordPropertyMetaField<M>>()

        recordClass.recordComponents.forEach { recordComponent ->
            val field = JavaRecordPropertyMetaField<M>(recordComponent)

            tempFields += field
            tempPropertiesToFields[field.fieldName] = field
        }

        fields = tempFields.toList()
        propertiesToFields = tempPropertiesToFields.toMap()
    }

    override fun findMetaFieldByName(fieldName: String): MetaField<M>? = propertiesToFields[fieldName]

    override fun createInstance(values: Array<Any?>): M? = recordConstructor.newInstance(*values)
}
