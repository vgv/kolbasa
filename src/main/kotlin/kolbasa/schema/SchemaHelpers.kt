package kolbasa.schema

import kolbasa.pg.DatabaseExtensions.useConnectionWithAutocommit
import kolbasa.queue.Queue
import javax.sql.DataSource

object SchemaHelpers {

    /**
     * Generate all statements needed to create/update database schema, but doesn't execute them
     */
    @JvmStatic
    fun generateDatabaseSchema(dataSource: DataSource, queues: List<Queue<*, *>>): Map<Queue<*, *>, Schema> {
        val existingTables = SchemaExtractor.extractRawSchema(dataSource, tableNamePattern = null)

        return queues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            SchemaGenerator.generateTableSchema(queue, existingTable)
        }
    }

    /**
     * Generate all statements needed to create/update database schema, but doesn't execute them
     */
    @JvmStatic
    fun generateDatabaseSchema(dataSource: DataSource, vararg queues: Queue<*, *>): Map<Queue<*, *>, Schema> {
        return generateDatabaseSchema(dataSource, queues.toList())
    }

    /**
     * Update database schema
     */
    @JvmStatic
    fun updateDatabaseSchema(dataSource: DataSource, queues: List<Queue<*, *>>) {
        generateDatabaseSchema(dataSource, queues).forEach { (_, schema) ->
            // we execute only required statements
            executeSchemaStatements(dataSource, schema.required)
        }
    }

    /**
     * Update database schema
     */
    @JvmStatic
    fun updateDatabaseSchema(dataSource: DataSource, vararg queues: Queue<*, *>) {
        updateDatabaseSchema(dataSource, queues.toList())
    }

    private fun executeSchemaStatements(dataSource: DataSource, statements: SchemaStatements) {
        if (statements.isEmpty()) {
            // nothing to execute
            return
        }

        dataSource.useConnectionWithAutocommit { connection ->
            // separate transaction for each statement
            connection.createStatement().use { statement ->
                statements.tableStatements.forEach(statement::execute)
                statements.indexStatements.forEach(statement::execute)
            }
        }
    }

}
