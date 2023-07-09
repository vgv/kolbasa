package kolbasa.queue.meta

import kolbasa.queue.Checks
import kolbasa.queue.Searchable
import kolbasa.queue.Unique
import kolbasa.schema.Const
import java.beans.PropertyDescriptor
import java.lang.reflect.RecordComponent
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.findAnnotation

internal abstract class MetaField<M : Any>(
    private val kotlinType: KClass<M>,
    val fieldName: String,
    searchable: Searchable?,
    unique: Unique?
) {

    init {
        Checks.checkMetaFieldName(fieldName)
        Checks.checkMetaFieldAnnotations(searchable, unique)
    }

    val dbColumnName = MetaHelpers.generateMetaColumnName(fieldName)
    val dbColumnType = platformTypeToDbType()
    private val sqlColumnType = platformTypeToJavaSqlType()
    val dbIndexType = defineIndexType(unique, searchable)
    private val enumValueOfFunction = MetaHelpers.findEnumValueOfFunction(kotlinType)

    abstract fun getValue(meta: M): Any?

    fun fillPreparedStatement(ps: PreparedStatement, columnIndex: Int, meta: M?) {
        val propertyValue = meta?.let { getValue(meta) }
        fillPreparedStatementForValue(ps, columnIndex, propertyValue)
    }

    fun fillPreparedStatementForValue(ps: PreparedStatement, columnIndex: Int, propertyValue: Any?) {
        if (propertyValue == null) {
            ps.setNull(columnIndex, sqlColumnType)
        } else {
            MetaHelpers.enumerateTypes(kotlinType,
                string = { ps.setString(columnIndex, propertyValue as String) },
                long = { ps.setLong(columnIndex, propertyValue as Long) },
                int = { ps.setInt(columnIndex, propertyValue as Int) },
                short = { ps.setShort(columnIndex, propertyValue as Short) },
                byte = { ps.setByte(columnIndex, propertyValue as Byte) },
                boolean = { ps.setBoolean(columnIndex, propertyValue as Boolean) },
                double = { ps.setDouble(columnIndex, propertyValue as Double) },
                float = { ps.setFloat(columnIndex, propertyValue as Float) },
                char = { ps.setString(columnIndex, (propertyValue as Char).toString()) },
                biginteger = { ps.setBigDecimal(columnIndex, (propertyValue as BigInteger).toBigDecimal()) },
                bigdecimal = { ps.setBigDecimal(columnIndex, propertyValue as BigDecimal) },
                enum = { ps.setString(columnIndex, (propertyValue as Enum<*>).name) }
            )
        }
    }

    fun readResultSet(rs: ResultSet, columnIndex: Int): Any? {
        val value: Any? = MetaHelpers.enumerateTypes(kotlinType,
            string = { rs.getString(columnIndex) },
            long = { rs.getLong(columnIndex) },
            int = { rs.getInt(columnIndex) },
            short = { rs.getShort(columnIndex) },
            byte = { rs.getByte(columnIndex) },
            boolean = { rs.getBoolean(columnIndex) },
            double = { rs.getDouble(columnIndex) },
            float = { rs.getFloat(columnIndex) },
            char = { rs.getString(columnIndex)[0] },
            biginteger = { rs.getBigDecimal(columnIndex).toBigInteger() },
            bigdecimal = { rs.getBigDecimal(columnIndex) },
            enum = {
                val enumStringValue = rs.getString(columnIndex)
                enumStringValue?.let { requireNotNull(enumValueOfFunction).call(enumStringValue) }
            }
        )

        return if (rs.wasNull()) {
            null
        } else {
            value
        }
    }

    private fun platformTypeToDbType(): String {
        return MetaHelpers.enumerateTypes(kotlinType,
            string = {
                "varchar(${Const.META_FIELD_STRING_TYPE_MAX_LENGTH})"
            },
            long = { "bigint" },
            int = { "int" },
            short = { "smallint" },
            byte = { "smallint" },
            boolean = { "boolean" },
            double = { "double precision" },
            float = { "real" },
            char = { "varchar(${Const.META_FIELD_CHAR_TYPE_MAX_LENGTH})" },
            biginteger = { "numeric" },
            bigdecimal = { "numeric" },
            enum = { "varchar(${Const.META_FIELD_ENUM_TYPE_MAX_LENGTH})" }
        )
    }

    private fun platformTypeToJavaSqlType(): Int {
        return MetaHelpers.enumerateTypes(kotlinType,
            string = { Types.VARCHAR },
            long = { Types.BIGINT },
            int = { Types.INTEGER },
            short = { Types.SMALLINT },
            byte = { Types.SMALLINT },
            boolean = { Types.BOOLEAN },
            double = { Types.DOUBLE },
            float = { Types.REAL },
            char = { Types.VARCHAR },
            biginteger = { Types.NUMERIC },
            bigdecimal = { Types.NUMERIC },
            enum = { Types.VARCHAR }
        )
    }

    private fun defineIndexType(unique: Unique?, searchable: Searchable?): MetaIndexType {
        return if (unique != null) {
            MetaIndexType.UNIQUE_INDEX
        } else if (searchable != null) {
            MetaIndexType.JUST_INDEX
        } else {
            MetaIndexType.NO_INDEX
        }
    }

}

@Suppress("UNCHECKED_CAST")
internal class KotlinPropertyMetaField<M : Any>(
    val property: KProperty1<M, *>
) :
    MetaField<M>(
        property.returnType.classifier as KClass<M>,
        property.name,
        property.findAnnotation(),
        property.findAnnotation()
    ) {

    override fun getValue(meta: M): Any? {
        return property(meta)
    }

}

@Suppress("UNCHECKED_CAST")
internal class JavaRecordPropertyMetaField<M : Any>(
    private val recordComponent: RecordComponent
) :
    MetaField<M>(
        recordComponent.type.kotlin as KClass<M>,
        recordComponent.name,
        recordComponent.getAnnotation(Searchable::class.java),
        recordComponent.getAnnotation(Unique::class.java)
    ) {

    override fun getValue(meta: M): Any? {
        return recordComponent.accessor(meta)
    }
}

@Suppress("UNCHECKED_CAST")
internal class JavaBeanMetaField<M : Any>(
    private val propertyDescriptor: PropertyDescriptor
) : MetaField<M>(
    propertyDescriptor.propertyType.kotlin as KClass<M>,
    propertyDescriptor.name,
    propertyDescriptor.propertyType.getAnnotation(Searchable::class.java),
    propertyDescriptor.propertyType.getAnnotation(Unique::class.java)
) {

    override fun getValue(meta: M): Any? {
        return propertyDescriptor.readMethod(meta)
    }
}

internal enum class MetaIndexType {
    NO_INDEX,
    JUST_INDEX,
    UNIQUE_INDEX
}
