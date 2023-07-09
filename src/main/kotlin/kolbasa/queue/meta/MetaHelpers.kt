package kolbasa.queue.meta

import kolbasa.schema.Const
import java.beans.Introspector
import java.lang.reflect.Constructor
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.staticFunctions
import kotlin.reflect.full.valueParameters

internal object MetaHelpers {

    private val META_COLUMN_REGEX = Regex("([a-z])([A-Z]+)")

    fun generateMetaColumnName(fieldName: String): String {
        // convert Java field into column name, like someField -> some_field
        val snakeCaseName = fieldName.replace(META_COLUMN_REGEX, "$1_$2").lowercase()
        // add 'meta_' prefix
        return Const.QUEUE_META_COLUMN_NAME_PREFIX + snakeCaseName
    }

    fun findEnumValueOfFunction(kClass: KClass<*>): KFunction<*>? {
        return kClass.staticFunctions.find { function ->
            if (function.name == "valueOf") {
                val arguments = function.valueParameters
                if (arguments.size == 1) {
                    if (arguments.first().type.classifier == String::class) {
                        return@find true
                    }
                }
            }

            false
        }
    }

    fun <R> enumerateTypes(
        type: KClass<*>,
        string: () -> R,
        long: () -> R,
        int: () -> R,
        short: () -> R,
        byte: () -> R,
        boolean: () -> R,
        double: () -> R,
        float: () -> R,
        char: () -> R,
        biginteger: () -> R,
        bigdecimal: () -> R,
        enum: () -> R
    ): R {
        return when {
            type == String::class -> string()
            type == Long::class -> long()
            type == Int::class -> int()
            type == Short::class -> short()
            type == Byte::class -> byte()
            type == Boolean::class -> boolean()
            type == Double::class -> double()
            type == Float::class -> float()
            type == Char::class -> char()
            type == BigInteger::class -> biginteger()
            type == BigDecimal::class -> bigdecimal()
            type.isSubclassOf(Enum::class) -> enum()
            else -> error("Type $type not supported")
        }
    }

    fun <T> findCanonicalRecordConstructor(recordClass: Class<T>): Constructor<T> {
        val componentTypes = recordClass.recordComponents
            .map { rc -> rc.type }
            .toTypedArray()

        return recordClass.getDeclaredConstructor(*componentTypes)
    }

    fun <T> findJavaBeanDefaultConstructor(clazz: Class<T>): Constructor<T> {
        val beanInfo = Introspector.getBeanInfo(clazz)
        val args = beanInfo.propertyDescriptors
            .filterNot { it.propertyType == Class::class.java }
            .map { propertyDescriptor ->
                propertyDescriptor.propertyType
            }
            .toTypedArray()

        return clazz.getDeclaredConstructor(*args)
    }

}
