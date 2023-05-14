package kolbasa.queue.meta

import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters

internal class KotlinMetaClass<M : Any>(kotlinClass: KClass<M>) : MetaClass<M>() {

    override val fields: List<KotlinPropertyMetaField<M>>

    private val primaryConstructor: KFunction<M>
    private val propertiesToFields: Map<String, KotlinPropertyMetaField<M>>

    init {
        check(kotlinClass.isData) {
            "Class $kotlinClass must be Kotlin data class"
        }

        // Find primary constructor
        primaryConstructor = requireNotNull(kotlinClass.primaryConstructor) {
            "Data class $kotlinClass without primary constructor. WAT?"
        }

        // Find data class properties
        val tempFields = mutableListOf<KotlinPropertyMetaField<M>>()
        val tempPropertiesToFields = mutableMapOf<String, KotlinPropertyMetaField<M>>()

        primaryConstructor.valueParameters.map { parameter ->
            val property = requireNotNull(kotlinClass.declaredMemberProperties.find { it.name == parameter.name }) {
                "Data class $kotlinClass has constructor parameter ${parameter.name}, but doesn't have such property. WAT?"
            }

            val field = KotlinPropertyMetaField(property)
            tempFields += field
            tempPropertiesToFields[field.fieldName] = field
        }

        fields = tempFields.toList()
        propertiesToFields = tempPropertiesToFields.toMap()
    }

    override fun findMetaFieldByName(fieldName: String): MetaField<M>? = propertiesToFields[fieldName]

    override fun createInstance(values: Array<Any?>): M = primaryConstructor.call(*values)
}
