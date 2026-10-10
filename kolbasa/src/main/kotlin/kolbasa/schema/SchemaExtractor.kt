package kolbasa.schema

import kolbasa.utils.JdbcHelpers
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.utils.JdbcHelpers.usePreparedStatement
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

            // Collect all indexes, identities and put functions
            val allIndexes = getAllIndexes(connection, connection.schema, tablesAndColumns.keys)
            val allIdentities = getIdentities(connection, connection.schema, tablesAndColumns.keys  )
            val allPutFunctions = getPutFunctions(connection, connection.schema, tablesAndColumns.keys)

            val result = mutableMapOf<TableName, Table>()
            for ((tableName, columns) in tablesAndColumns) {
                val indexes = allIndexes[tableName] ?: emptySet()
                val identity = allIdentities[tableName]
                val putFunction = allPutFunctions[tableName]
                result[tableName] = Table(tableName, columns, indexes, identity, putFunction)
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
        tableNames: Set<TableName>
    ): Map<TableName, SequenceName> {
        if (tableNames.isEmpty()) {
            return emptyMap()
        }

        val allSequenceNamesQuery = """
            with tbl_names(table_name) as (select unnest(?::text[]))
            select c.relname as table_name, s.relname as sequence_name
            from tbl_names
            join pg_namespace n on n.nspname = ?
            join pg_class c on c.relnamespace = n.oid and c.relname = tbl_names.table_name
            join pg_attribute a on a.attrelid = c.oid
                and a.attname = ?
                and not a.attisdropped
            join pg_depend d on d.refobjid = c.oid
                and d.refobjsubid = a.attnum
                and d.classid = 'pg_class'::regclass
            join pg_class s on s.oid = d.objid and s.relkind = 'S'
        """

        val sequenceNames = mutableMapOf<TableName, SequenceName>()
        connection.usePreparedStatement(allSequenceNamesQuery) { preparedStatement ->
            preparedStatement.setArray(1, connection.createArrayOf("text", tableNames.toTypedArray()))
            preparedStatement.setString(2, JdbcHelpers.schemaNameOrDefault(schemaName))
            preparedStatement.setString(3, Const.ID_COLUMN_NAME)

            preparedStatement.executeQuery().use { resultSet ->
                while (resultSet.next()) {
                    val tableName = resultSet.getString(1)
                    val sequenceName = resultSet.getString(2)
                    sequenceNames[tableName] = sequenceName
                }
            }
        }
        return sequenceNames
    }

    // Reads the q_<table>_put functions for the given tables (function name = tableName + "_put"). The
    // kolbasa content hash is parsed from each function's COMMENT ('kolbasa-put:<md5>'); null if it has none.
    private fun getPutFunctions(
        connection: Connection,
        schemaName: String?,
        tableNames: Set<TableName>
    ): Map<TableName, PutFunction> {
        if (tableNames.isEmpty()) {
            return emptyMap()
        }

        val realSchemaName = JdbcHelpers.schemaNameOrDefault(schemaName)
        // function name -> table name, e.g. ["q_users_put" -> "q_users"]
        val functionToTable = tableNames.associateBy { it + Const.PUT_FUNCTION_NAME_SUFFIX }

        val sql = """
            select
                p.proname as name,
                d.description as description
            from pg_proc p
            join pg_namespace n on n.oid = p.pronamespace
            left join pg_description d
                on d.objoid = p.oid and d.classoid = 'pg_proc'::regclass and d.objsubid = 0
            where
                n.nspname = '$realSchemaName' and
                p.proname in (${functionToTable.keys.joinToString(separator = ",") { "'$it'" }})
        """

        val result = mutableMapOf<TableName, PutFunction>()
        connection.useStatement(sql) { resultSet ->
            while (resultSet.next()) {
                val functionName = resultSet.getString("name")
                val tableName = functionToTable[functionName] ?: continue
                val description = resultSet.getString("description")
                val hash = description
                    ?.takeIf { it.startsWith(Const.PUT_FUNCTION_COMMENT_PREFIX) }
                    ?.removePrefix(Const.PUT_FUNCTION_COMMENT_PREFIX)
                result[tableName] = PutFunction(functionName, hash)
            }
        }
        return result
    }

}
