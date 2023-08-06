package kolbasa.queue.meta

import java.lang.reflect.Constructor

internal class JavaRecordMetaClass<Meta : Any>(recordClass: Class<Meta>) : MetaClass<Meta>() {

    override val fields: List<JavaRecordPropertyMetaField<Meta>>

    private val recordConstructor: Constructor<Meta>
    private val propertiesToFields: Map<String, JavaRecordPropertyMetaField<Meta>>

    init {
        check(recordClass.isRecord) {
            "Class $recordClass must be a Java record class"
        }

        recordConstructor = MetaHelpers.findCanonicalRecordConstructor(recordClass)

        val tempFields = mutableListOf<JavaRecordPropertyMetaField<Meta>>()
        val tempPropertiesToFields = mutableMapOf<String, JavaRecordPropertyMetaField<Meta>>()

        recordClass.recordComponents.forEach { recordComponent ->
            val field = JavaRecordPropertyMetaField<Meta>(recordComponent)

            tempFields += field
            tempPropertiesToFields[field.fieldName] = field
        }

        fields = tempFields.toList()
        propertiesToFields = tempPropertiesToFields.toMap()
    }

    override fun findMetaFieldByName(fieldName: String): MetaField<Meta>? = propertiesToFields[fieldName]

    override fun createInstance(values: Array<Any?>): Meta? = recordConstructor.newInstance(*values)
}
