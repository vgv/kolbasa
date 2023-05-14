package kolbasa.schema

import kolbasa.queue.Queue
import javax.sql.DataSource

object SchemaHelpers {

    /**
     * Generate all statements needed to create/update database schema, but doesn't execute them
     */
    @JvmStatic
    fun generateDatabaseSchema(dataSource: DataSource, vararg queues: Queue<*, *>): Map<Queue<*, *>, Schema> {
        val existingTables = SchemaExtractor.extractRawSchema(dataSource, tableNamePattern = null)

        return queues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            SchemaGenerator.generateTableSchema(queue, existingTable)
        }
    }

    /**
     * Update database schema
     */
    @JvmStatic
    fun updateDatabaseSchema(dataSource: DataSource, vararg queues: Queue<*, *>) {
        generateDatabaseSchema(dataSource, *queues).forEach { (_, schema) ->
            // we execute only required statements
            executeSchemaStatements(dataSource, schema.required)
        }
    }

    private fun executeSchemaStatements(dataSource: DataSource, statements: SchemaStatements) {
        if (statements.isEmpty()) {
            // nothing to execute
            return
        }

        dataSource.connection.use { connection ->
            connection.autoCommit = true // separate transaction for each statement
            connection.createStatement().use { statement ->
                statements.tableStatements.forEach(statement::execute)
                statements.indexStatements.forEach(statement::execute)
            }
        }
    }

}
