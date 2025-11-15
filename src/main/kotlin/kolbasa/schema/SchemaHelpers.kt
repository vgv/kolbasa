package kolbasa.schema

import kolbasa.pg.DatabaseExtensions.useConnectionWithAutocommit
import kolbasa.queue.Queue
import javax.sql.DataSource

object SchemaHelpers {

    /**
     * Generate all statements needed to create/update database schema but doesn't execute them
     */
    @JvmStatic
    fun generateDatabaseSchema(dataSource: DataSource, queues: List<Queue<*>>): Map<Queue<*>, Schema> {
        val node = IdSchema.readNodeInfo(dataSource)
        val idRange = if (node != null) {
            // This server is a part of a clustered environment, currently or in the past
            // We can't know for sure, but we have to restrict the range of identifiers
            IdRange.generateRange(node.identifiersBucket)
        } else {
            // This server is not a part of a clustered environment, so, we can use all [0...Long.MAX_VALUE] range
            IdRange.LOCAL_RANGE
        }

        val existingTables = SchemaExtractor.extractRawSchema(dataSource, queues.map { it.dbTableName }.toSet())

        return queues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            SchemaGenerator.generateTableSchema(queue, existingTable, idRange)
        }
    }

    /**
     * Generate all statements needed to create/update database schema but doesn't execute them
     */
    @JvmStatic
    fun generateDatabaseSchema(dataSource: DataSource, vararg queues: Queue<*>): Map<Queue<*>, Schema> {
        return generateDatabaseSchema(dataSource, queues.toList())
    }

    /**
     * Update database schema.
     *
     * Every kolbasa queue has its own real table in the database. This method creates or updates the table schema for the queue.
     * When we want to use a queue, we may have the following situations:
     * 1) This is the first use of the queue and the table in the database simply does not exist, it must be created from scratch
     * 2) The queue has already been used, the table in the database exists, but since the last use, the queue metadata
     *    has changed and one or more columns/indexes must be added to the table in the database
     * 3) The queue has not changed, but the internal data representation in kolbasa itself has changed and several service
     *    columns/indexes must be added/removed
     * 4) The queue has not changed, the table in the database is up-to-date, nothing needs to be done
     *
     * This is a convenient method that allows you to simply bring the table in the database to the current queue state,
     * making the correct data migration for each of the above cases
     */
    @JvmStatic
    fun updateDatabaseSchema(dataSource: DataSource, queues: List<Queue<*>>) {
        generateDatabaseSchema(dataSource, queues).forEach { (_, schema) ->
            // we execute only required statements
            executeSchemaStatements(dataSource, schema.required)
        }
    }

    /**
     * Update database schema
     *
     * See [updateDatabaseSchema] for more details
     */
    @JvmStatic
    fun updateDatabaseSchema(dataSource: DataSource, vararg queues: Queue<*>) {
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
