package kolbasa.schema

import kolbasa.pg.DatabaseExtensions.readString
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.pg.DatabaseExtensions.useStatement
import java.sql.Connection
import java.sql.DatabaseMetaData
import javax.sql.DataSource

internal object SchemaExtractor {

    /**
     * Extracts raw schema information from the database
     *
     * @param dataSource data source to extract schema from
     * @param queueNames if specified, only tables with these names will be extracted, otherwise all queue tables will be extracted
     * @return map of table name to table definition
     */
    internal fun extractRawSchema(dataSource: DataSource, queueNames: Set<String>? = null): Map<String, Table> {
        // only tables that match the queue name pattern just to reduce the amount of data we need to process
        val tableNamePattern = Const.QUEUE_TABLE_NAME_PREFIX + "%"

        return dataSource.useConnection { connection ->
            val tables = mutableMapOf<String, Table>()

            // Collect all indexes
            val allIndexes = getAllIndexes(connection.schema, tableNamePattern, connection)

            // Collect all table names
            for ((tableName, columns) in getAllColumns(connection.schema, tableNamePattern, connection.metaData)) {
                if (queueNames != null && tableName !in queueNames) {
                    // continue only for specific tables
                    continue
                }

                if (!checkColumns(columns)) {
                    // table columns don't look like a queue table
                    continue
                }

                val identity = getIdentity(connection, connection.schema, tableName)
                    ?: continue // every queue table should have an identity column

                val indexes = allIndexes[tableName]
                    ?: continue // every queue table should have indexes

                tables[tableName] = Table(tableName, columns, indexes, identity)
            }

            return@useConnection tables
        }
    }

    private fun getAllColumns(
        schemaName: String?,
        tableNamePattern: String,
        databaseMetaData: DatabaseMetaData
    ): Map<String, Set<Column>> {
        val result = mutableMapOf<String, MutableSet<Column>>()

        val columnsResultSet = databaseMetaData.getColumns(null, schemaName, tableNamePattern, null)
        while (columnsResultSet.next()) {
            val tableName = columnsResultSet.getString("TABLE_NAME")
            val columnName = columnsResultSet.getString("COLUMN_NAME")
            val columnType = columnsResultSet.getString("TYPE_NAME")
            val columnDefault = columnsResultSet.getString("COLUMN_DEF")
            val columnNullable = columnsResultSet.getInt("NULLABLE")

            val parsedColumnType = ColumnType.fromDbType(columnType)
            if (parsedColumnType != null) {
                val column = Column(columnName, parsedColumnType, columnNullable == 1, columnDefault)
                result.compute(tableName) { _, existingColumns ->
                    if (existingColumns == null) {
                        mutableSetOf(column)
                    } else {
                        existingColumns.add(column)
                        existingColumns
                    }
                }
            }
        }

        return result
    }

    private fun getAllIndexes(schemaName: String?, tableNamePattern: String, connection: Connection): Map<String, Set<String>> {
        val result = mutableMapOf<String, MutableSet<String>>()

        val realSchemaName = schemaName ?: "public"
        val sql =
            "select tablename,indexname from pg_indexes where schemaname='$realSchemaName' and tablename like '$tableNamePattern'"

        connection.useStatement(sql) { resultSet ->
            while (resultSet.next()) {
                val tableName = resultSet.getString("tablename")
                val indexName = resultSet.getString("indexname")

                result.compute(tableName) { _, existingIndexes ->
                    if (existingIndexes == null) {
                        mutableSetOf(indexName)
                    } else {
                        existingIndexes += indexName
                        existingIndexes
                    }
                }
            }
        }

        return result
    }

    private fun getIdentity(connection: Connection, schemaName: String?, tableName: String): Identity? {
        val realSchemaName = schemaName ?: "public"
        val realSchemaNameWithDot = "$realSchemaName."

        val sequenceName = connection
            .readString("select pg_get_serial_sequence('$realSchemaName.$tableName', '${Const.ID_COLUMN_NAME}')")
            .removePrefix(realSchemaNameWithDot)

        val sequenceQuery = """
            select seqstart,
                   seqmin,
                   seqmax,
                   seqincrement,
                   seqcycle,
                   seqcache
            from pg_catalog.pg_sequence
            where
                seqrelid in (
                    select pg_class.oid from pg_class
                    left join pg_namespace on (pg_namespace.oid = pg_class.relnamespace)
                    where
                        pg_class.relname='$sequenceName' and
                        pg_namespace.nspname='$realSchemaName'
                );
        """.trimIndent()

        return connection.useStatement { statement ->
            statement.executeQuery(sequenceQuery).use { resultSet ->
                if (resultSet.next()) {
                    Identity(
                        "$realSchemaName.$sequenceName",
                        start = resultSet.getLong("seqstart"),
                        min = resultSet.getLong("seqmin"),
                        max = resultSet.getLong("seqmax"),
                        increment = resultSet.getLong("seqincrement"),
                        cycles = resultSet.getBoolean("seqcycle"),
                        cache = resultSet.getLong("seqcache")
                    )
                } else {
                    null
                }
            }
        }
    }

    private fun checkColumns(columns: Set<Column>): Boolean {
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
}
