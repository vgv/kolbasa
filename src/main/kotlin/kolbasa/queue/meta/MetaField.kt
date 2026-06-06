package kolbasa.queue.meta

import kolbasa.queue.Checks
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

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
 * | [instant]        | [Instant]      | `timestamptz`          |
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
    internal val dbColumnArrayBaseType: String,
    internal val sqlColumnType: Int
) {

    internal abstract val dbColumnName: String
    internal abstract val dbIndexType: MetaIndexType

    abstract fun value(value: T): MetaValue<T>

    fun withOption(newOption: FieldOption): MetaField<T> {
        @Suppress("UNCHECKED_CAST")
        return when (this) {
            is ByteField -> copy(option = newOption)
            is ShortField -> copy(option = newOption)
            is IntField -> copy(option = newOption)
            is LongField -> copy(option = newOption)
            is BooleanField -> copy(option = newOption)
            is FloatField -> copy(option = newOption)
            is DoubleField -> copy(option = newOption)
            is StringField -> copy(option = newOption)
            is BigIntegerField -> copy(option = newOption)
            is BigDecimalField -> copy(option = newOption)
            is InstantField -> copy(option = newOption)
        } as MetaField<T>
    }

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
            val mapped = arrayOfNulls<Any?>(propertyValues.size)
            propertyValues.forEachIndexed { index, value ->
                @Suppress("UNCHECKED_CAST")
                mapped[index] = if (value == null) null else toJdbcArrayElement(value as T)
            }
            val sqlArray = ps.connection.createArrayOf(dbColumnArrayBaseType, mapped)
            ps.setArray(columnIndex, sqlArray)
        }
    }

    /**
     * Hook for types whose JDBC array-element representation differs from the Kotlin user type.
     * Default is identity, so all 10 numeric/boolean/string types are byte-for-byte unchanged.
     * [InstantField] overrides this to map [Instant] → [OffsetDateTime] (UTC) because pgjdbc
     * has no [Instant] writer for `timestamptz` arrays.
     */
    protected open fun toJdbcArrayElement(value: T): Any = value as Any

    companion object {

        /**
         * A meta-field holding a [Byte], stored as PostgreSQL `smallint` (PostgreSQL has no
         * single-byte integer type, so `byte` and [short] share the same column type).
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `smallint` column
         */
        fun byte(name: String, option: FieldOption = FieldOption.NONE): MetaField<Byte> {
            Checks.checkUserDefinedMetaFieldName(name)
            return ByteField(name, option)
        }

        /**
         * A meta-field holding a [Short], stored as PostgreSQL `smallint`.
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `smallint` column
         */
        fun short(name: String, option: FieldOption = FieldOption.NONE): MetaField<Short> {
            Checks.checkUserDefinedMetaFieldName(name)
            return ShortField(name, option)
        }

        /**
         * A meta-field holding an [Int], stored as PostgreSQL `int`.
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by an `int` column
         */
        fun int(name: String, option: FieldOption = FieldOption.NONE): MetaField<Int> {
            Checks.checkUserDefinedMetaFieldName(name)
            return IntField(name, option)
        }

        /**
         * A meta-field holding a [Long], stored as PostgreSQL `bigint`.
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `bigint` column
         */
        fun long(name: String, option: FieldOption = FieldOption.NONE): MetaField<Long> {
            Checks.checkUserDefinedMetaFieldName(name)
            return LongField(name, option)
        }

        /**
         * A meta-field holding a [Boolean], stored as PostgreSQL `boolean`.
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `boolean` column
         */
        fun boolean(name: String, option: FieldOption = FieldOption.NONE): MetaField<Boolean> {
            Checks.checkUserDefinedMetaFieldName(name)
            return BooleanField(name, option)
        }

        /**
         * A meta-field holding a [Float], stored as PostgreSQL `real` (single-precision).
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `real` column
         */
        fun float(name: String, option: FieldOption = FieldOption.NONE): MetaField<Float> {
            Checks.checkUserDefinedMetaFieldName(name)
            return FloatField(name, option)
        }

        /**
         * A meta-field holding a [Double], stored as PostgreSQL `double precision`.
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `double precision` column
         */
        fun double(name: String, option: FieldOption = FieldOption.NONE): MetaField<Double> {
            Checks.checkUserDefinedMetaFieldName(name)
            return DoubleField(name, option)
        }

        /**
         * A meta-field holding a [String], stored as PostgreSQL `varchar`.
         *
         * The column is capped at [Const.META_FIELD_STRING_TYPE_MAX_LENGTH] characters; longer
         * values are rejected by the database.
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `varchar` column
         */
        fun string(name: String, option: FieldOption = FieldOption.NONE): MetaField<String> {
            Checks.checkUserDefinedMetaFieldName(name)
            return StringField(name, option)
        }

        /**
         * A meta-field holding a [BigInteger], stored as PostgreSQL `numeric` (arbitrary
         * precision; `bigInteger` and [bigDecimal] share the same column type).
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `numeric` column
         */
        fun bigInteger(name: String, option: FieldOption = FieldOption.NONE): MetaField<BigInteger> {
            Checks.checkUserDefinedMetaFieldName(name)
            return BigIntegerField(name, option)
        }

        /**
         * A meta-field holding a [BigDecimal], stored as PostgreSQL `numeric` (arbitrary
         * precision and scale).
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `numeric` column
         */
        fun bigDecimal(name: String, option: FieldOption = FieldOption.NONE): MetaField<BigDecimal> {
            Checks.checkUserDefinedMetaFieldName(name)
            return BigDecimalField(name, option)
        }

        /**
         * A meta-field holding a moment in time — [java.time.Instant], stored as PostgreSQL
         * `timestamptz`. The value is a UTC instant: a lossless, timezone-independent round-trip
         * through the column.
         *
         * Precision: `timestamptz` is microsecond. [Instant.now] carries nanos; the sub-µs
         * portion is truncated on the way to the database. If you bind
         * `2026-05-25T14:30:00.123456789Z` you will read back `2026-05-25T14:30:00.123456Z`.
         *
         * Range: `timestamptz` covers `4713 BC … 294276 AD`. Sending [Instant.MIN]/[Instant.MAX]
         * will fail at the driver/database boundary — these are not validated client-side.
         *
         * ```kotlin
         * val EVENT_TIME = MetaField.instant("event_time", FieldOption.SEARCH)
         *
         * producer.send(queue,
         *     SendMessage("payload", meta = MetaValues.of(EVENT_TIME.value(Instant.now()))))
         *
         * consumer.receive(queue, 10, ReceiveOptions(
         *     filter = EVENT_TIME greater Instant.now().minusSeconds(60)))
         * ```
         *
         * @param name the field name, used as the base to generate a column name
         * @param option the field option controlling indexing and uniqueness behavior
         * @return a [MetaField] backed by a `timestamptz` column
         */
        fun instant(name: String, option: FieldOption = FieldOption.NONE): MetaField<Instant> {
            Checks.checkUserDefinedMetaFieldName(name)
            return InstantField(name, option)
        }
    }

}

data class ByteField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Byte>(
    name = name,
    option = option,
    dbColumnType = "smallint",
    dbColumnArrayBaseType = "smallint",
    sqlColumnType = Types.SMALLINT
) {

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
) : MetaField<Short>(
    name = name,
    option = option,
    dbColumnType = "smallint",
    dbColumnArrayBaseType = "smallint",
    sqlColumnType = Types.SMALLINT
) {

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
) : MetaField<Int>(
    name = name,
    option = option,
    dbColumnType = "int",
    dbColumnArrayBaseType = "int",
    sqlColumnType = Types.INTEGER
) {

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
) : MetaField<Long>(
    name = name,
    option = option,
    dbColumnType = "bigint",
    dbColumnArrayBaseType = "bigint",
    sqlColumnType = Types.BIGINT
) {

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
) : MetaField<Boolean>(
    name = name,
    option = option,
    dbColumnType = "boolean",
    dbColumnArrayBaseType = "boolean",
    sqlColumnType = Types.BOOLEAN
) {

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
) : MetaField<Float>(
    name = name,
    option = option,
    dbColumnType = "real",
    dbColumnArrayBaseType = "real",
    sqlColumnType = Types.REAL
) {

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
) : MetaField<Double>(
    name = name,
    option = option,
    dbColumnType = "double precision",
    dbColumnArrayBaseType = "double precision",
    sqlColumnType = Types.DOUBLE
) {

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
) : MetaField<String>(
    name = name,
    option = option,
    dbColumnType = "varchar(${Const.META_FIELD_STRING_TYPE_MAX_LENGTH})",
    dbColumnArrayBaseType = "varchar",
    sqlColumnType = Types.VARCHAR
) {

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
) : MetaField<BigInteger>(
    name = name,
    option = option,
    dbColumnType = "numeric",
    dbColumnArrayBaseType = "numeric",
    sqlColumnType = Types.NUMERIC
) {

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
) : MetaField<BigDecimal>(
    name = name,
    option = option,
    dbColumnType = "numeric",
    dbColumnArrayBaseType = "numeric",
    sqlColumnType = Types.NUMERIC
) {

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

data class InstantField internal constructor(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Instant>(
    name = name,
    option = option,
    dbColumnType = "timestamptz",
    dbColumnArrayBaseType = "timestamptz",
    sqlColumnType = Types.TIMESTAMP_WITH_TIMEZONE
) {

    init {
        Checks.checkMetaFieldName(name)
    }

    override val dbColumnName = QueueHelpers.generateMetaColumnDbName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Instant): MetaValue<Instant> = InstantValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Instant? {
        // pgjdbc 42.7 has no java.time.Instant handler; OffsetDateTime is the JSR-310 bridge for timestamptz.
        return rs.getObject(columnIndex, OffsetDateTime::class.java)?.toInstant()
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Instant) {
        ps.setObject(columnIndex, OffsetDateTime.ofInstant(value, ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
    }

    override fun toJdbcArrayElement(value: Instant): Any {
        return OffsetDateTime.ofInstant(value, ZoneOffset.UTC)
    }
}
