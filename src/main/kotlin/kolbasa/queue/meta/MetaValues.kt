package kolbasa.queue.meta

import java.math.BigDecimal
import java.math.BigInteger

/**
 * A container that holds a collection of [MetaValue] instances for a queue message.
 *
 * MetaValues groups all metadata field values together, providing type-safe access to individual
 * values by their corresponding [MetaField] definitions.
 *
 * ## Creating MetaValues
 *
 * ```kotlin
 * // Using vararg factory method
 * val meta = MetaValues.of(
 *     userId.value(12345L),
 *     priority.value(10),
 *     category.value("orders")
 * )
 *
 * // Using list factory method
 * val meta = MetaValues.of(listOf(userId.value(12345L)))
 *
 * // Empty metadata
 * val empty = MetaValues.EMPTY
 * ```
 *
 * ## Retrieving Values
 *
 * ```kotlin
 * // Get value (throws if not found)
 * val id: Long = meta.get(userId)
 *
 * // Get value or null
 * val id: Long? = meta.getOrNull(userId)
 * ```
 *
 * @property values the list of metadata values in this container
 *
 * @see MetaValue
 * @see MetaField
 */
data class MetaValues(val values: List<MetaValue<*>>) {

    private val fieldsToValues: Map<MetaField<*>, MetaValue<*>> = values.associateBy { it.field }

    private fun <T> findValue(field: MetaField<T>): MetaValue<T>? {
        val metaValue = fieldsToValues[field]

        return if (metaValue != null) {
            @Suppress("UNCHECKED_CAST")
            metaValue as MetaValue<T>
        } else {
            null
        }
    }

    fun <T> getOrNull(field: MetaField<T>): T? {
        return findValue(field)?.value
    }

    fun <T> get(field: MetaField<T>): T {
        val value = requireNotNull(findValue(field)) {
            "Field $field not found. Known fields: ${values.map { it.field }}"
        }

        return value.value
    }


    companion object {
        val EMPTY = of(emptyList())

        fun of(values: List<MetaValue<*>>) = MetaValues(values)
        fun of(vararg values: MetaValue<*>) = of(values.toList())
    }
}


/**
 * Represents a typed value bound to a specific [MetaField].
 *
 * MetaValue pairs a [MetaField] definition with an actual value, ensuring type safety
 * between the field declaration and the data it holds. Instances are created through
 * the [MetaField.value] method, not directly.
 *
 * ## Supported Types
 *
 * | Value Class        | Kotlin Type    |
 * |--------------------|----------------|
 * | [ByteValue]        | [Byte]         |
 * | [ShortValue]       | [Short]        |
 * | [IntValue]         | [Int]          |
 * | [LongValue]        | [Long]         |
 * | [BooleanValue]     | [Boolean]      |
 * | [FloatValue]       | [Float]        |
 * | [DoubleValue]      | [Double]       |
 * | [StringValue]      | [String]       |
 * | [BigIntegerValue]  | [BigInteger]   |
 * | [BigDecimalValue]  | [BigDecimal]   |
 *
 * ## Usage Example
 *
 * ```kotlin
 * // Define a meta field
 * val priority = MetaField.int("priority", FieldOption.SEARCH)
 *
 * // Create a value (binding) using the field's value() method
 * val priorityValue: MetaValue<Int> = priority.value(10)
 *
 * // Access the underlying data
 * val field: MetaField<Int> = priorityValue.field
 * val value: Int = priorityValue.value
 * ```
 *
 * @param T the type of value this instance holds
 * @property field the metadata field definition this value belongs to
 * @property value the actual typed value
 *
 * @see MetaField
 * @see MetaValues
 */
sealed class MetaValue<T>(
    open val field: MetaField<T>,
    open val value: T
)

data class ByteValue internal constructor(
    override val field: MetaField<Byte>,
    override val value: Byte
) : MetaValue<Byte>(field, value)

data class ShortValue internal constructor(
    override val field: MetaField<Short>,
    override val value: Short
) : MetaValue<Short>(field, value)

data class IntValue internal constructor(
    override val field: MetaField<Int>,
    override val value: Int
) : MetaValue<Int>(field, value)

data class LongValue internal constructor(
    override val field: MetaField<Long>,
    override val value: Long
) : MetaValue<Long>(field, value)

data class BooleanValue internal constructor(
    override val field: MetaField<Boolean>,
    override val value: Boolean
) : MetaValue<Boolean>(field, value)

data class FloatValue internal constructor(
    override val field: MetaField<Float>,
    override val value: Float
) : MetaValue<Float>(field, value)

data class DoubleValue internal constructor(
    override val field: MetaField<Double>,
    override val value: Double
) : MetaValue<Double>(field, value)

data class StringValue internal constructor(
    override val field: MetaField<String>,
    override val value: String
) : MetaValue<String>(field, value)

data class BigIntegerValue internal constructor(
    override val field: MetaField<BigInteger>,
    override val value: BigInteger
) : MetaValue<BigInteger>(field, value)

data class BigDecimalValue internal constructor(
    override val field: MetaField<BigDecimal>,
    override val value: BigDecimal
) : MetaValue<BigDecimal>(field, value)
