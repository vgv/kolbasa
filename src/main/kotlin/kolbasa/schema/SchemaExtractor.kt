package kolbasa.schema

import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.utils.JdbcHelpers.useStatement
import java.sql.Connection
import java.sql.DatabaseMetaData
import javax.sql.DataSource

// Just to make a difference between different String usages
private typealias SequenceName = String
private typealias IndexName = String
private typealias TableName = String

internal object SchemaExtractor {

    /**
     * Extracts raw schema information from the database
     *
     * @param dataSource data source to extract schema from
     * @param queueNames if specified, only tables with these names will be extracted, otherwise all queue tables will be extracted
     * @return map of table name to table definition
     */
    internal fun extractRawSchema(dataSource: DataSource, queueNames: Set<String>? = null): Map<TableName, Table> {
        return dataSource.useConnection { connection ->
            val tablesAndColumns = mutableMapOf<TableName, Set<Column>>()

            // Collect all the names of tables that are queues
            for ((tableName, columns) in getAllColumns(connection.schema, connection.metaData)) {
                if (queueNames != null && tableName !in queueNames) {
                    // continue only for specific tables
                    continue
                }

                if (!isQueueTable(columns)) {
                    // table columns don't look like a queue table
                    continue
                }

                tablesAndColumns[tableName] = columns
            }

            // Collect all indexes and identities
            val allIndexes = getAllIndexes(connection, connection.schema, tablesAndColumns.keys)
            val allIdentities = getIdentities(connection, connection.schema, tablesAndColumns.keys)

            val result = mutableMapOf<TableName, Table>()
            for ((tableName, columns) in tablesAndColumns) {
                val identity = allIdentities[tableName]
                    ?: continue // every queue table should have an identity column

                val indexes = allIndexes[tableName]
                    ?: continue // every queue table should have indexes

                result[tableName] = Table(tableName, columns, indexes, identity)
            }
            result
        }
    }

    private fun getAllColumns(
        schemaName: String?,
        databaseMetaData: DatabaseMetaData
    ): Map<TableName, Set<Column>> {
        // only tables that match the queue name pattern to reduce the amount of data we need to process
        val tableNamePattern = Const.QUEUE_TABLE_NAME_PREFIX + "%"

        val result = mutableMapOf<TableName, MutableSet<Column>>()

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

    private fun getAllIndexes(
        connection: Connection,
        schemaName: String?,
        tableNames: Set<TableName>
    ): Map<TableName, Set<IndexName>> {
        if (tableNames.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<TableName, MutableSet<IndexName>>()

        val realSchemaName = schemaName ?: "public"
        val sql = """
            select tablename,indexname from pg_indexes where
                schemaname='$realSchemaName' and
                tablename in (${tableNames.joinToString(separator = ",") { "'$it'" }})"""

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

    private fun getIdentities(connection: Connection, schemaName: String?, tableNames: Set<TableName>): Map<TableName, Identity> {
        if (tableNames.isEmpty()) {
            return emptyMap()
        }

        val realSchemaName = schemaName ?: "public"

        val sequenceNames = findSequences(connection, schemaName, tableNames)

        val sequenceQuery = """
            select
                pg_class.relname as sequence_name,
                pg_sequence.seqstart,
                pg_sequence.seqmin,
                pg_sequence.seqmax,
                pg_sequence.seqincrement,
                pg_sequence.seqcycle,
                pg_sequence.seqcache
            from pg_sequence
            inner join pg_class on (pg_class.oid = pg_sequence.seqrelid)
            inner join pg_namespace on (pg_namespace.oid = pg_class.relnamespace)
            where
                pg_namespace.nspname='$realSchemaName' and
                pg_class.relname in (${sequenceNames.values.joinToString(separator = ",") { "'$it'" }})
        """

        val identities: Map<SequenceName, Identity> = connection.useStatement(sequenceQuery) { resultSet ->
            val result = mutableMapOf<SequenceName, Identity>()
            while (resultSet.next()) {
                val sequenceName = resultSet.getString("sequence_name")
                val identity = Identity(
                    name = "$realSchemaName.$sequenceName",
                    start = resultSet.getLong("seqstart"),
                    min = resultSet.getLong("seqmin"),
                    max = resultSet.getLong("seqmax"),
                    increment = resultSet.getLong("seqincrement"),
                    cycles = resultSet.getBoolean("seqcycle"),
                    cache = resultSet.getLong("seqcache")
                )

                result[sequenceName] = identity
            }
            result
        }

        // Merge tables and sequences
        val result = mutableMapOf<TableName, Identity>()
        sequenceNames.forEach { (tableName, sequenceName) ->
            val identity = identities[sequenceName]
            if (identity != null) {
                result[tableName] = identity
            }
        }
        return result
    }

    private fun findSequences(
        connection: Connection,
        schemaName: String?,
        tableNames: Set<String>
    ): Map<TableName, SequenceName> {
        if (tableNames.isEmpty()) {
            return emptyMap()
        }

        val realSchemaNameWithDot = if (schemaName == null) {
            "public."
        } else {
            "${schemaName}."
        }

        val allSequenceNamesQuery = """
            with tbl_names(table_name) as (values ${tableNames.joinToString(separator = ",") { "('$it')" }})
            select table_name, pg_get_serial_sequence(table_name,'${Const.ID_COLUMN_NAME}') from (table tbl_names) as tbl_names
        """

        val sequenceNames = mutableMapOf<TableName, SequenceName>()
        connection.useStatement(allSequenceNamesQuery) { resultSet ->
            while (resultSet.next()) {
                val tableName = resultSet.getString(1)
                val sequenceName = resultSet.getString(2).removePrefix(realSchemaNameWithDot)
                sequenceNames[tableName] = sequenceName
            }
        }
        return sequenceNames
    }

    private fun isQueueTable(columns: Set<Column>): Boolean {
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
