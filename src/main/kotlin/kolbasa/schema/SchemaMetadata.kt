package kolbasa.schema

import java.sql.Types

data class Schema(
    val all: SchemaStatements,
    val required: SchemaStatements
)

data class SchemaStatements(
    /**
     * All "create table", "alter column", "set default" etc. statements
     */
    val tableStatements: List<String>,
    /**
     * All "create" and "drop index" statements
     */
    val indexStatements: List<String>
) {
    fun isEmpty() = tableStatements.isEmpty() && indexStatements.isEmpty()
}

internal data class Table(
    val name: String,
    val columns: Set<Column>,
    val indexes: Set<Index>,
    val identity: Identity
) {
    fun findColumn(name: String): Column? = columns.find { it.name == name }
    fun findIndex(name: String): Index? = indexes.find { it.name == name }
}

internal data class Column(
    val name: String,
    val type: ColumnType,
    val nullable: Boolean,
    val defaultExpression: String?
)

internal enum class ColumnType(
    val dbTypes: Set<String>,
    val sqlType: Int
) {
    SMALLINT(setOf("int2"), Types.SMALLINT),
    INT(setOf("int4"), Types.INTEGER),
    BIGINT(setOf("int8"), Types.BIGINT),
    REAL(setOf("float4"), Types.REAL),
    DOUBLE(setOf("float8"), Types.DOUBLE),
    NUMERIC(setOf("numeric"), Types.NUMERIC),
    BOOLEAN(setOf("bool"), Types.BOOLEAN),
    TIMESTAMP(setOf("timestamp"), Types.TIMESTAMP),
    VARCHAR(setOf("varchar", "text"), Types.VARCHAR),
    VARCHAR_ARRAY(setOf("_varchar"), Types.ARRAY),
    BYTEARRAY(setOf("bytea"), Types.BINARY),
    JSONB(setOf("jsonb"), Types.OTHER);

    companion object {
        fun fromDbType(dbType: String): ColumnType? = values().find { dbType in it.dbTypes }
    }
}

internal data class Identity(
    val name: String,
    val start: Long,
    val min: Long,
    val max: Long,
    val increment: Long,
    val cycles: Boolean,
    val cache: Long
)

internal data class Index(
    val name: String,
    val unique: Boolean,
    val columns: List<IndexColumn>,
    val filterCondition: String?,
    val invalid: Boolean
)

internal data class IndexColumn(
    val name: String,
    val asc: Boolean
)
