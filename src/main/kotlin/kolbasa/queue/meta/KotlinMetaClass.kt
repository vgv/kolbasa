package kolbasa.queue.meta

import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KVisibility
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters

internal class KotlinMetaClass<Meta : Any>(kotlinClass: KClass<Meta>) : MetaClass<Meta>() {

    override val fields: List<KotlinPropertyMetaField<Meta>>

    private val primaryConstructor: KFunction<Meta>
    private val propertiesToFields: Map<String, KotlinPropertyMetaField<Meta>>

    init {
        check(kotlinClass.isData) {
            "Class $kotlinClass must be Kotlin data class"
        }

        check(kotlinClass.visibility != KVisibility.PRIVATE) {
            "Class $kotlinClass must not be private"
        }

        // Find primary constructor
        primaryConstructor = requireNotNull(kotlinClass.primaryConstructor) {
            "Data class $kotlinClass without primary constructor. WAT?"
        }

        // Find data class properties
        val tempFields = mutableListOf<KotlinPropertyMetaField<Meta>>()
        val tempPropertiesToFields = mutableMapOf<String, KotlinPropertyMetaField<Meta>>()

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

    override fun findMetaFieldByName(fieldName: String): MetaField<Meta>? = propertiesToFields[fieldName]

    override fun createInstance(values: Array<Any?>): Meta = primaryConstructor.call(*values)
}
