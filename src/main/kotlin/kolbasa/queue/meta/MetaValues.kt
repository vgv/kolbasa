package kolbasa.queue.meta

import java.math.BigDecimal
import java.math.BigInteger

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


sealed class MetaValue<T>(
    open val field: MetaField<T>,
    open val value: T
)

internal data class ByteValue(
    override val field: MetaField<Byte>,
    override val value: Byte
) : MetaValue<Byte>(field, value)

internal data class ShortValue(
    override val field: MetaField<Short>,
    override val value: Short
) : MetaValue<Short>(field, value)

internal data class IntValue(
    override val field: MetaField<Int>,
    override val value: Int
) : MetaValue<Int>(field, value)

internal data class LongValue(
    override val field: MetaField<Long>,
    override val value: Long
) : MetaValue<Long>(field, value)

internal data class BooleanValue(
    override val field: MetaField<Boolean>,
    override val value: Boolean
) : MetaValue<Boolean>(field, value)

internal data class FloatValue(
    override val field: MetaField<Float>,
    override val value: Float
) : MetaValue<Float>(field, value)

internal data class DoubleValue(
    override val field: MetaField<Double>,
    override val value: Double
) : MetaValue<Double>(field, value)

internal data class StringValue(
    override val field: MetaField<String>,
    override val value: String
) : MetaValue<String>(field, value)

internal data class BigIntegerValue(
    override val field: MetaField<BigInteger>,
    override val value: BigInteger
) : MetaValue<BigInteger>(field, value)

internal data class BigDecimalValue(
    override val field: MetaField<BigDecimal>,
    override val value: BigDecimal
) : MetaValue<BigDecimal>(field, value)
