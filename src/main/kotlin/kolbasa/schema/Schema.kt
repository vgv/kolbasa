package kolbasa.schema

import java.sql.Types

data class Schema(
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

    companion object {
        val EMPTY = Schema(emptyList(), emptyList())

        operator fun Schema.plus(other: Schema): Schema {
            val tableStatements = this.tableStatements + other.tableStatements
            val indexStatements = this.indexStatements + other.indexStatements

            return Schema(tableStatements, indexStatements)
        }
    }

}


internal data class Table(
    val name: String,
    val columns: Set<Column>,
    val indexes: Set<String>,
    val identity: Identity
) {
    fun findColumn(name: String): Column? = columns.find { it.name == name }

    companion object {
        fun Table?.hasIndex(name: String): Boolean {
            return if (this == null) {
                false
            } else {
                name in indexes
            }
        }
    }
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
        fun fromDbType(dbType: String): ColumnType? = ColumnType.entries.find { dbType in it.dbTypes }
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
