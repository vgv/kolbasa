package kolbasa.schema

import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.useConnection
import java.sql.Connection
import java.sql.DatabaseMetaData
import javax.sql.DataSource

internal object SchemaExtractor {

    internal fun extractRawSchema(dataSource: DataSource, tableNamePattern: String? = null): Map<String, Table> {
        return dataSource.useConnection { connection ->
            val databaseMetaData = connection.metaData

            val tablesResultSet = databaseMetaData.getTables(null, connection.schema, tableNamePattern, arrayOf("TABLE"))
            val tables = mutableMapOf<String, Table>()
            while (tablesResultSet.next()) {
                val tableName = tablesResultSet.getString("TABLE_NAME")

                val columns = getAllColumns(connection.schema, tableName, databaseMetaData)
                val indexes = getAllIndexes(connection.schema, tableName, databaseMetaData)

                tables[tableName] = Table(tableName, columns, indexes)
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

            result += Column(columnName, columnType, columnNullable == 1, columnDefault)
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

}
