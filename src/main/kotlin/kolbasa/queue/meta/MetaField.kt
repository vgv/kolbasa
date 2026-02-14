package kolbasa.queue.meta

import kolbasa.queue.Checks
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

/**
 * Represents a typed metadata field that can be attached to queue message.
 *
 * Meta fields allow you to store additional typed information alongside your queue messages.
 * This metadata can be used for filtering, sorting, and deduplication of messages without
 * deserializing the message payload (which can be quite expensive).
 *
 * ## Supported Types
 *
 * | Factory Method   | Kotlin Type    | PostgreSQL Type        |
 * |------------------|----------------|------------------------|
 * | [byte]           | [Byte]         | `smallint`             |
 * | [short]          | [Short]        | `smallint`             |
 * | [int]            | [Int]          | `int`                  |
 * | [long]           | [Long]         | `bigint`               |
 * | [boolean]        | [Boolean]      | `boolean`              |
 * | [float]          | [Float]        | `real`                 |
 * | [double]         | [Double]       | `double precision`     |
 * | [string]         | [String]       | `varchar`              |
 * | [bigInteger]     | [BigInteger]   | `numeric`              |
 * | [bigDecimal]     | [BigDecimal]   | `numeric`              |
 *
 * ## Field Options
 *
 * Each field can be created with a [FieldOption] that controls indexing and uniqueness:
 * - [FieldOption.NONE] - No index, just stores data (most efficient)
 * - [FieldOption.SEARCH] - Creates an index for filtering/sorting
 * - [FieldOption.ALL_LIVE_UNIQUE] - Unique constraint across all live messages (`SCHEDULED` + `READY` + `IN_FLIGHT` + `RETRY`)
 * - [FieldOption.UNTOUCHED_UNIQUE] - Unique constraint only for "untouched" messages (`SCHEDULED` + `READY`)
 *
 * ## Usage Example
 *
 * ```kotlin
 * // Define meta fields
 * val accountId = MetaField.long("account_id", FieldOption.SEARCH)
 * val priority = MetaField.int("priority", FieldOption.SEARCH)
 * val deduplicationKey = MetaField.string("deduplication_key", FieldOption.ALL_LIVE_UNIQUE)
 *
 * // Create queue with meta fields
 * val queue = Queue.of(
 *     name = "orders",
 *     metadata = listOf(userId, priority, deduplicationKey),
 *     ...
 * )
 *
 * // Send message with meta values
 * producer.send(
 *     queue,
 *     SendMessage("payload_data", meta = MetaValues.of(
 *         userId.value(12345L),
 *         priority.value(10),
 *         deduplicationKey.value("unique-key-001")
 *     ))
 * )
 * ```
 *
 * @param T the type of value this field holds
 * @property name the field name, used as the base to generate a column name in a database
 * @property option the field option controlling indexing and uniqueness behavior
 *
 * @see Metadata
 * @see FieldOption
 * @see MetaValue
 */
sealed class MetaField<T>(
    open val name: String,
    open val option: FieldOption,
    internal val dbColumnType: String,
    internal val sqlColumnType: Int
) {

    internal abstract val dbColumnName: String
    internal abstract val dbIndexType: MetaIndexType

    abstract fun value(value: T): MetaValue<T>

    protected abstract fun read(rs: ResultSet, columnIndex: Int): T?

    protected abstract fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: T)

    // ------------------------------------------------

    internal fun readValue(rs: ResultSet, columnIndex: Int): MetaValue<T>? {
        val result = read(rs, columnIndex)
        return if (rs.wasNull() || result == null) {
            null
        } else {
            value(result)
        }
    }

    internal fun fillPreparedStatementForValue(ps: PreparedStatement, columnIndex: Int, metaValues: MetaValues) {
        val value = metaValues.getOrNull(this)
        fillPreparedStatementForValue(ps, columnIndex, value)
    }

    internal fun fillPreparedStatementForValue(ps: PreparedStatement, columnIndex: Int, value: T?) {
        if (value == null) {
            ps.setNull(columnIndex, sqlColumnType)
        } else {
            fillPreparedStatement(ps, columnIndex, value)
        }
    }

    internal fun fillPreparedStatementForValues(ps: PreparedStatement, columnIndex: Int, propertyValues: Collection<*>?) {
        if (propertyValues == null) {
            ps.setNull(columnIndex, sqlColumnType)
        } else {
            val sqlArray = ps.connection.createArrayOf(dbColumnType, propertyValues.toTypedArray())
            ps.setArray(columnIndex, sqlArray)
        }
    }

    companion object {

        fun byte(name: String, option: FieldOption = FieldOption.NONE): MetaField<Byte> =
            ByteField(name, option)

        fun short(name: String, option: FieldOption = FieldOption.NONE): MetaField<Short> =
            ShortField(name, option)

        fun int(name: String, option: FieldOption = FieldOption.NONE): MetaField<Int> =
            IntField(name, option)

        fun long(name: String, option: FieldOption = FieldOption.NONE): MetaField<Long> =
            LongField(name, option)

        fun boolean(name: String, option: FieldOption = FieldOption.NONE): MetaField<Boolean> =
            BooleanField(name, option)

        fun float(name: String, option: FieldOption = FieldOption.NONE): MetaField<Float> =
            FloatField(name, option)

        fun double(name: String, option: FieldOption = FieldOption.NONE): MetaField<Double> =
            DoubleField(name, option)

        fun string(name: String, option: FieldOption = FieldOption.NONE): MetaField<String> =
            StringField(name, option)

        fun bigInteger(name: String, option: FieldOption = FieldOption.NONE): MetaField<BigInteger> =
            BigIntegerField(name, option)

        fun bigDecimal(name: String, option: FieldOption = FieldOption.NONE): MetaField<BigDecimal> =
            BigDecimalField(name, option)
    }

}

data class ByteField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Byte>(name, option, "smallint", Types.SMALLINT) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Byte): MetaValue<Byte> = ByteValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Byte {
        return rs.getByte(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Byte) {
        ps.setByte(columnIndex, value)
    }
}

data class ShortField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Short>(name, option, "smallint", Types.SMALLINT) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Short): MetaValue<Short> = ShortValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Short {
        return rs.getShort(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Short) {
        ps.setShort(columnIndex, value)
    }
}

data class IntField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Int>(name, option, "int", Types.INTEGER) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Int): MetaValue<Int> = IntValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Int {
        return rs.getInt(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Int) {
        ps.setInt(columnIndex, value)
    }
}

data class LongField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Long>(name, option, "bigint", Types.BIGINT) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Long): MetaValue<Long> = LongValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Long {
        return rs.getLong(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Long) {
        ps.setLong(columnIndex, value)
    }
}

data class BooleanField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Boolean>(name, option, "boolean", Types.BOOLEAN) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Boolean): MetaValue<Boolean> = BooleanValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Boolean {
        return rs.getBoolean(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Boolean) {
        ps.setBoolean(columnIndex, value)
    }
}

data class FloatField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Float>(name, option, "real", Types.REAL) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Float): MetaValue<Float> = FloatValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Float {
        return rs.getFloat(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Float) {
        ps.setFloat(columnIndex, value)
    }
}

data class DoubleField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Double>(name, option, "double precision", Types.DOUBLE) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Double): MetaValue<Double> = DoubleValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Double {
        return rs.getDouble(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Double) {
        ps.setDouble(columnIndex, value)
    }
}

data class StringField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<String>(name, option, "varchar(${Const.META_FIELD_STRING_TYPE_MAX_LENGTH})", Types.VARCHAR) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: String): MetaValue<String> = StringValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): String? {
        return rs.getString(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: String) {
        ps.setString(columnIndex, value)
    }
}

data class BigIntegerField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<BigInteger>(name, option, "numeric", Types.NUMERIC) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: BigInteger): MetaValue<BigInteger> = BigIntegerValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): BigInteger? {
        return rs.getBigDecimal(columnIndex)?.toBigInteger()
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: BigInteger) {
        ps.setBigDecimal(columnIndex, value.toBigDecimal())
    }
}

data class BigDecimalField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<BigDecimal>(name, option, "numeric", Types.NUMERIC) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: BigDecimal): MetaValue<BigDecimal> = BigDecimalValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): BigDecimal? {
        return rs.getBigDecimal(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: BigDecimal) {
        ps.setBigDecimal(columnIndex, value)
    }
}
