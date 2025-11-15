package kolbasa.queue.meta

import kolbasa.schema.Const
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

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

        fun char(name: String, option: FieldOption = FieldOption.NONE): MetaField<Char> =
            CharField(name, option)

        fun string(name: String, option: FieldOption = FieldOption.NONE): MetaField<String> =
            StringField(name, option)

        fun bigInteger(name: String, option: FieldOption = FieldOption.NONE): MetaField<BigInteger> =
            BigIntegerField(name, option)

        fun bigDecimal(name: String, option: FieldOption = FieldOption.NONE): MetaField<BigDecimal> =
            BigDecimalField(name, option)

        fun <E : Enum<E>> enum(
            name: String,
            type: Class<E>,
            searchable: FieldOption = FieldOption.NONE
        ): MetaField<E> = EnumField(name, type, searchable)

    }

}

private data class ByteField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Byte>(name, option, "smallint", Types.SMALLINT) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Byte): ByteValue = ByteValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Byte {
        return rs.getByte(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Byte) {
        ps.setByte(columnIndex, value)
    }
}

private data class ShortField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Short>(name, option, "smallint", Types.SMALLINT) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Short): ShortValue = ShortValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Short {
        return rs.getShort(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Short) {
        ps.setShort(columnIndex, value)
    }
}

private data class IntField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Int>(name, option, "int", Types.INTEGER) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Int): IntValue = IntValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Int {
        return rs.getInt(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Int) {
        ps.setInt(columnIndex, value)
    }
}

private data class LongField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Long>(name, option, "bigint", Types.BIGINT) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Long): LongValue = LongValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Long {
        return rs.getLong(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Long) {
        ps.setLong(columnIndex, value)
    }
}

private data class BooleanField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Boolean>(name, option, "boolean", Types.BOOLEAN) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Boolean): BooleanValue = BooleanValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Boolean {
        return rs.getBoolean(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Boolean) {
        ps.setBoolean(columnIndex, value)
    }
}

private data class FloatField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Float>(name, option, "real", Types.REAL) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Float): FloatValue = FloatValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Float {
        return rs.getFloat(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Float) {
        ps.setFloat(columnIndex, value)
    }
}

private data class DoubleField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Double>(name, option, "double precision", Types.DOUBLE) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Double): DoubleValue = DoubleValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Double {
        return rs.getDouble(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Double) {
        ps.setDouble(columnIndex, value)
    }
}

private data class CharField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<Char>(name, option, "varchar(${Const.META_FIELD_CHAR_TYPE_MAX_LENGTH})", Types.VARCHAR) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: Char): CharValue = CharValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): Char? {
        val value = rs.getString(columnIndex)
        return if (value == null || value.isEmpty()) {
            null
        } else {
            value[0]
        }
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: Char) {
        ps.setString(columnIndex, value.toString())
    }
}

private data class StringField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<String>(name, option, "varchar(${Const.META_FIELD_STRING_TYPE_MAX_LENGTH})", Types.VARCHAR) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: String): StringValue = StringValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): String? {
        return rs.getString(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: String) {
        ps.setString(columnIndex, value)
    }
}

private data class BigIntegerField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<BigInteger>(name, option, "numeric", Types.NUMERIC) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: BigInteger): BigIntegerValue = BigIntegerValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): BigInteger? {
        return rs.getBigDecimal(columnIndex)?.toBigInteger()
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: BigInteger) {
        ps.setBigDecimal(columnIndex, value.toBigDecimal())
    }
}

private data class BigDecimalField(
    override val name: String,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<BigDecimal>(name, option, "numeric", Types.NUMERIC) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: BigDecimal): BigDecimalValue = BigDecimalValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): BigDecimal? {
        return rs.getBigDecimal(columnIndex)
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: BigDecimal) {
        ps.setBigDecimal(columnIndex, value)
    }
}

private data class EnumField<E : Enum<E>>(
    override val name: String,
    val type: Class<E>,
    override val option: FieldOption = FieldOption.NONE
) : MetaField<E>(name, option, "varchar(${Const.META_FIELD_ENUM_TYPE_MAX_LENGTH})", Types.VARCHAR) {

    override val dbColumnName = MetaHelpers.generateMetaColumnName(name)
    override val dbIndexType = MetaHelpers.defineIndexType(option)

    override fun value(value: E): EnumValue<E> = EnumValue(this, value)

    override fun read(rs: ResultSet, columnIndex: Int): E? {
        val textValue = rs.getString(columnIndex)

        return if (textValue == null) {
            null
        } else {
            java.lang.Enum.valueOf<E>(type, textValue)
        }
    }

    override fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, value: E) {
        ps.setString(columnIndex, value.name)
    }
}
