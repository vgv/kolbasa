package kolbasa.schema

import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.readString
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.pg.DatabaseExtensions.useStatement
import java.sql.Connection
import java.sql.DatabaseMetaData
import javax.sql.DataSource

internal object SchemaExtractor {

    internal fun extractRawSchema(dataSource: DataSource, queueNames: Set<String>): Map<String, Table> {
        // only tables that match the queue name pattern just to reduce the amount of data we need to process
        val tableNamePattern = Const.QUEUE_TABLE_NAME_PREFIX + "%"

        return dataSource.useConnection { connection ->
            val databaseMetaData = connection.metaData

            val tablesResultSet = databaseMetaData.getTables(null, connection.schema, tableNamePattern, arrayOf("TABLE"))
            val tables = mutableMapOf<String, Table>()
            while (tablesResultSet.next()) {
                val tableName = tablesResultSet.getString("TABLE_NAME")
                if (tableName !in queueNames) {
                    // continue only for specific tables
                    continue
                }

                val columns = getAllColumns(connection.schema, tableName, databaseMetaData)
                val indexes = getAllIndexes(connection.schema, tableName, databaseMetaData)
                val identity = getIdentity(connection, connection.schema, tableName)

                tables[tableName] = Table(tableName, columns, indexes, identity)
            }

            return@useConnection tables
        }
    }

    private fun getAllColumns(schemaName: String?, tableName: String, databaseMetaData: DatabaseMetaData): Set<Column> {
        val columnsResultSet = databaseMetaData.getColumns(null, schemaName, tableName, null)
        val result = mutableSetOf<Column>()

        while (columnsResultSet.next()) {
            val columnName = columnsResultSet.getString("COLUMN_NAME")
            val columnType = columnsResultSet.getString("TYPE_NAME")
            val columnDefault = columnsResultSet.getString("COLUMN_DEF")
            val columnNullable = columnsResultSet.getInt("NULLABLE")

            val parsedColumnType = requireNotNull(ColumnType.fromDbType(columnType)) {
                """Unsupported column type "$columnType" for column "$columnName" in table "$tableName"."""
            }
            result += Column(columnName, parsedColumnType, columnNullable == 1, columnDefault)
        }

        return result
    }

    private fun getAllIndexes(schemaName: String?, tableName: String, databaseMetaData: DatabaseMetaData): Set<Index> {
        val indexesResultSet = databaseMetaData.getIndexInfo(null, schemaName, tableName, false, false)
        val result = mutableMapOf<String, Index>()

        while (indexesResultSet.next()) {
            val indexName = indexesResultSet.getString("INDEX_NAME")
            val column = indexesResultSet.getString("COLUMN_NAME")
            val unique = !indexesResultSet.getBoolean("NON_UNIQUE")
            val filterCondition = indexesResultSet.getString("FILTER_CONDITION")
            val invalid = isIndexInvalid(databaseMetaData.connection, schemaName, indexName)
            val asc = indexesResultSet.getString("ASC_OR_DESC") == "A"

            result.compute(indexName) { _, existingDefinition ->
                if (existingDefinition == null) {
                    Index(indexName, unique, listOf(IndexColumn(column, asc)), filterCondition, invalid)
                } else {
                    val newColumns = existingDefinition.columns + IndexColumn(column, asc)
                    existingDefinition.copy(columns = newColumns)
                }
            }
        }

        return result.values.toSet()
    }

    private fun isIndexInvalid(connection: Connection, schemaName: String?, indexName: String): Boolean {
        val query = """
            select count(*) from pg_namespace
            inner join pg_class on (pg_namespace.oid = pg_class.relnamespace)
            inner join pg_index on (pg_class.oid = pg_index.indexrelid)
            where
                pg_namespace.nspname = '${schemaName ?: "public"}' and
                pg_class.relname='$indexName' and
                pg_index.indisvalid = false
        """.trimIndent()

        return connection.readInt(query) > 0
    }

    private fun getIdentity(connection: Connection, schemaName: String?, tableName: String): Identity {
        val realSchemaName = schemaName ?: "public"
        val realSchemaNameWithDot = "$realSchemaName."

        val sequenceName = connection
            .readString("select pg_get_serial_sequence('$realSchemaName.$tableName', '${Const.ID_COLUMN_NAME}')")
            .removePrefix(realSchemaNameWithDot)

        val seqQeury = """
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

        val identity = connection.useStatement { statement ->
            statement.executeQuery(seqQeury).use { resultSet ->
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

        return requireNotNull(identity) {
            "Identity for table $tableName not found"
        }
    }

}
