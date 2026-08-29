package kolbasa.schema

import java.sql.Types

/**
 * The SQL statements that bring the database in line with a queue – built, but not executed yet.
 *
 * Kolbasa builds a schema by comparing the queue you declared with the table that exists in the database right now.
 * If the table is missing, the schema creates it. If the table is there but a meta field or an index was added since
 * the last run, the schema contains only what is missing. If everything is already in place, the schema is empty
 * ([isEmpty] is `true`).
 *
 * Typically, you don't need [Schema] object itself, because the application normally works with
 * [SchemaHelpers.createOrUpdateQueues] method, which creates [Schema] and executes it immediately. However, if you're
 * debugging, performing research, or your application has some other way of handling database migrations, you may need to
 * work with [Schema] directly.
 *
 * In the example below, an application retrieves [Schema], logs all queries, and then executes them using
 * standard Kolbasa [SchemaHelpers.executeSchemaStatements] method.
 *
 * ## Example
 *
 * ```kotlin
 * // extract all schemas for 'orders' and 'customers' queues
 * // No database changes, generateCreateOrUpdateStatements() method only reads the database metadata
 * val schemas = SchemaHelpers.generateCreateOrUpdateStatements(dataSource, orders, customers)
 * // schemas has one schema per queue, merge all these schemas into one big schema
 * val mergedSchema = schemas.values.merge()
 * // dump all statements
 * mergedSchema.tableStatements.forEach(::println)
 * mergedSchema.indexStatements.forEach(::println)
 * // execute all statements
 * val schemaResult = SchemaHelpers.executeSchemaStatements(dataSource, mergedSchema)
 *
 * if (schemaResult.failedStatements > 0) {
 *   schemaResult.failedTableStatements.forEach { println("${it.statement} -> ${it.error}") }
 *   schemaResult.failedIndexStatements.forEach { println("${it.statement} -> ${it.error}") }
 * }
 * ```
 *
 * @see SchemaHelpers
 * @see SchemaResult
 */
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

    /** How many statements this schema holds in total – table statements plus index statements. */
    val size = (tableStatements.size + indexStatements.size)

    /** `true` when there is nothing to do, because the database already matches the queue. */
    val isEmpty = (size == 0)

    companion object {
        internal val EMPTY = Schema(emptyList(), emptyList())

        /**
         * Merge multiple schemas into one
         */
        @JvmStatic
        fun Iterable<Schema>.merge(): Schema {
            val tableStatements = sumOf { it.tableStatements.size }
            val indexStatements = sumOf { it.indexStatements.size }

            if (tableStatements == 0 && indexStatements == 0) {
                return EMPTY
            } else {
                val mergedTableStatements = flatMapTo(ArrayList(tableStatements)) { it.tableStatements }
                val mergedIndexStatements = flatMapTo(ArrayList(indexStatements)) { it.indexStatements }
                return Schema(mergedTableStatements, mergedIndexStatements)
            }
        }
    }
}


internal data class Table(
    val name: String,
    val columns: Set<Column>,
    val indexes: Set<String>,
    // Table identity column, if exists
    val identity: Identity?,
    // SQL put function, if exists
    val putFunction: PutFunction? = null
) {
    fun findColumn(name: String): Column? = columns.find { it.name == name }

    fun isQueueTable(): Boolean {
        if (identity == null) {
            // every queue table should have an identity on 'id' column
            return false
        }

        val allRequiredColumnsExist = REQUIRED_QUEUE_COLUMNS.all { requiredColumn ->
            val currentColumn = columns.find { it.name == requiredColumn.key }
            currentColumn != null && currentColumn.type == requiredColumn.value
        }

        if (!allRequiredColumnsExist) {
            return false
        }

        // For 'data' column we can't check a type, because it can have different types
        // Check only the name
        val dataColumn = columns.find { it.name == Const.DATA_COLUMN_NAME }
        return dataColumn != null
    }

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
    TIMESTAMPTZ(setOf("timestamptz"), Types.TIMESTAMP_WITH_TIMEZONE),
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

internal data class PutFunction(
    /**
     * SQL put function (q_<name>_put) name
     */
    val name: String,
    /**
     * Function content hash, used for reliable change detection. Stored in the function's COMMENT ('kolbasa-put:<md5>')
     */
    val hash: String?
)

// Every queue table should have these columns
private val REQUIRED_QUEUE_COLUMNS = mapOf(
    Const.ID_COLUMN_NAME to ColumnType.BIGINT,
    Const.USELESS_COUNTER_COLUMN_NAME to ColumnType.INT,
    Const.OPENTELEMETRY_COLUMN_NAME to ColumnType.VARCHAR_ARRAY,
    Const.SHARD_COLUMN_NAME to ColumnType.INT,
    Const.CREATED_AT_COLUMN_NAME to ColumnType.TIMESTAMP,
    Const.SCHEDULED_AT_COLUMN_NAME to ColumnType.TIMESTAMP,
    Const.PROCESSING_AT_COLUMN_NAME to ColumnType.TIMESTAMP,
    Const.PRODUCER_COLUMN_NAME to ColumnType.VARCHAR,
    Const.CONSUMER_COLUMN_NAME to ColumnType.VARCHAR,
    Const.REMAINING_ATTEMPTS_COLUMN_NAME to ColumnType.INT
)
