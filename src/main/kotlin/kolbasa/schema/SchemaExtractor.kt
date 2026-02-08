package kolbasa.schema

import kolbasa.utils.JdbcHelpers
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.utils.JdbcHelpers.useStatement
import java.sql.Connection
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
     * @param tableNames if specified, only tables with these names will be extracted, otherwise all tables will be extracted
     * @return map of table name to table definition
     */
    internal fun extractRawSchema(dataSource: DataSource, tableNames: Set<String>? = null): Map<TableName, Table> {
        return dataSource.useConnection { connection ->
            val tablesAndColumns = mutableMapOf<TableName, Set<Column>>()

            // Collect all the names of tables
            for ((tableName, columns) in getAllColumns(connection, connection.schema, tableNames)) {
                if (tableNames != null && tableName !in tableNames) {
                    // continue only for specific tables
                    continue
                }

                tablesAndColumns[tableName] = columns
            }

            // Collect all indexes and identities
            val allIndexes = getAllIndexes(connection, connection.schema, tablesAndColumns.keys)
            val allIdentities = getIdentities(connection, connection.schema, tablesAndColumns.keys)

            val result = mutableMapOf<TableName, Table>()
            for ((tableName, columns) in tablesAndColumns) {
                val indexes = allIndexes[tableName] ?: emptySet()
                val identity = allIdentities[tableName]
                result[tableName] = Table(tableName, columns, indexes, identity)
            }
            result
        }
    }

    private fun getAllColumns(
        connection: Connection,
        schemaName: String?,
        tableNames: Set<TableName>?
    ): Map<TableName, Set<Column>> {
        val result = mutableMapOf<TableName, MutableSet<Column>>()

        val realSchemaName = JdbcHelpers.schemaNameOrDefault(schemaName)
        val tableNamesClause = if (!tableNames.isNullOrEmpty()) {
            "and table_name in (${tableNames.joinToString(separator = ",") { "'$it'" }})"
        } else {
            ""
        }

        val sql = """
            select
                table_name,
                column_name,
                udt_name,
                column_default,
                is_nullable
            from information_schema.columns
            where
                table_schema='$realSchemaName' $tableNamesClause
        """

        connection.useStatement(sql) { resultSet ->
            while (resultSet.next()) {
                val tableName = resultSet.getString("table_name")
                val columnName = resultSet.getString("column_name")
                val columnType = resultSet.getString("udt_name")
                val columnDefault = resultSet.getString("column_default")
                val columnNullable = resultSet.getString("is_nullable")

                val parsedColumnType = ColumnType.fromDbType(columnType)
                if (parsedColumnType != null) {
                    val column = Column(columnName, parsedColumnType, columnNullable == "YES", columnDefault)
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

        val realSchemaName = JdbcHelpers.schemaNameOrDefault(schemaName)
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

    private fun getIdentities(
        connection: Connection,
        schemaName: String?,
        tableNames: Set<TableName>
    ): Map<TableName, Identity> {
        if (tableNames.isEmpty()) {
            return emptyMap()
        }

        val sequenceNames = findSequences(connection, schemaName, tableNames)
        if (sequenceNames.isEmpty()) {
            return emptyMap()
        }

        val realSchemaName = JdbcHelpers.schemaNameOrDefault(schemaName)
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

        val realSchemaNameWithDot = "${JdbcHelpers.schemaNameOrDefault(schemaName)}."
        val allSequenceNamesQuery = """
            with tbl_names(table_name) as (values ${tableNames.joinToString(separator = ",") { "('$it')" }})
            select table_name, pg_get_serial_sequence(table_name,'${Const.ID_COLUMN_NAME}') from (table tbl_names) as tbl_names
        """

        val sequenceNames = mutableMapOf<TableName, SequenceName>()
        connection.useStatement(allSequenceNamesQuery) { resultSet ->
            while (resultSet.next()) {
                val tableName = resultSet.getString(1)
                val sequenceName = resultSet.getString(2)
                if (sequenceName != null) {
                    sequenceNames[tableName] = sequenceName.removePrefix(realSchemaNameWithDot)
                }
            }
        }
        return sequenceNames
    }
}
