package kolbasa.queue.meta

import java.beans.Introspector
import java.lang.reflect.Constructor

internal class JavaBeanMetaClass<Meta : Any>(javaClass: Class<Meta>) : MetaClass<Meta>() {

    override val fields: List<MetaField<Meta>>

    private val javaBeanConstructor: Constructor<Meta>
    private val propertiesToFields: Map<String, MetaField<Meta>>

    init {
        javaBeanConstructor = MetaHelpers.findJavaBeanDefaultConstructor(javaClass)


        val tempFields = mutableListOf<JavaBeanMetaField<Meta>>()
        val tempPropertiesToFields = mutableMapOf<String, JavaBeanMetaField<Meta>>()

        val beanDescriptor = Introspector.getBeanInfo(javaClass)
        beanDescriptor.propertyDescriptors
            .filterNot { it.propertyType == Class::class.java }
            .forEach { propertyDescriptor ->
                val field = JavaBeanMetaField<Meta>(propertyDescriptor)

                tempFields += field
                tempPropertiesToFields[field.fieldName] = field
            }

        fields = tempFields.toList()
        propertiesToFields = tempPropertiesToFields.toMap()
    }

    override fun findMetaFieldByName(fieldName: String): MetaField<Meta>? = propertiesToFields[fieldName]

    override fun createInstance(values: Array<Any?>): Meta? = javaBeanConstructor.newInstance(*values)
}
