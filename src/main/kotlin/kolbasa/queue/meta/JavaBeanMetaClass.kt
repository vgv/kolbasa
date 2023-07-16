package kolbasa.queue.meta

import java.beans.Introspector
import java.lang.reflect.Constructor

internal class JavaBeanMetaClass<M : Any>(javaClass: Class<M>) : MetaClass<M>() {

    override val fields: List<MetaField<M>>

    private val javaBeanConstructor: Constructor<M>
    private val propertiesToFields: Map<String, MetaField<M>>

    init {
        javaBeanConstructor = MetaHelpers.findJavaBeanDefaultConstructor(javaClass)


        val tempFields = mutableListOf<JavaBeanMetaField<M>>()
        val tempPropertiesToFields = mutableMapOf<String, JavaBeanMetaField<M>>()

        val beanDescriptor = Introspector.getBeanInfo(javaClass)
        beanDescriptor.propertyDescriptors
            .filterNot { it.propertyType == Class::class.java }
            .forEach { propertyDescriptor ->
                val field = JavaBeanMetaField<M>(propertyDescriptor)

                tempFields += field
                tempPropertiesToFields[field.fieldName] = field
            }

        fields = tempFields.toList()
        propertiesToFields = tempPropertiesToFields.toMap()
    }

    override fun findMetaFieldByName(fieldName: String): MetaField<M>? = propertiesToFields[fieldName]

    override fun createInstance(values: Array<Any?>): M? = javaBeanConstructor.newInstance(*values)
}
