package kolbasa.schema

import kolbasa.utils.JdbcHelpers.useConnectionWithAutocommit
import kolbasa.queue.Queue
import kolbasa.schema.Schema.Companion.merge
import kolbasa.schema.SchemaHelpers.createOrUpdateQueues
import kolbasa.schema.SchemaHelpers.deleteQueues
import kolbasa.schema.SchemaHelpers.renameQueues
import javax.sql.DataSource

object SchemaHelpers {

    // ----------------------------------------------------------------------------------------
    // Create/Update functions

    /**
     * Generate all statements needed to create/update database schema but doesn't execute them
     */
    @JvmStatic
    fun generateCreateOrUpdateStatements(dataSource: DataSource, queues: List<Queue<*>>): Map<Queue<*>, Schema> {
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
    fun generateCreateOrUpdateStatements(dataSource: DataSource, vararg queues: Queue<*>): Map<Queue<*>, Schema> {
        return generateCreateOrUpdateStatements(dataSource, queues.toList())
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
    fun createOrUpdateQueues(dataSource: DataSource, queues: List<Queue<*>>): SchemaResult {
        val mergedSchema = generateCreateOrUpdateStatements(dataSource, queues).values.merge()
        return executeSchemaStatements(dataSource, mergedSchema)
    }

    /**
     * Update database schema
     *
     * See [createOrUpdateQueues] for more details
     */
    @JvmStatic
    fun createOrUpdateQueues(dataSource: DataSource, vararg queues: Queue<*>): SchemaResult {
        return createOrUpdateQueues(dataSource, queues.toList())
    }

    // ----------------------------------------------------------------------------------------
    // Rename functions
    /**
     * Generate all statements needed to rename queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateRenameStatements(
        dataSource: DataSource,
        queues: List<Queue<*>>,
        renameFunction: (Queue<*>) -> String
    ): Map<Queue<*>, Schema> {
        val existingTables = SchemaExtractor.extractRawSchema(dataSource, queues.map { it.dbTableName }.toSet())

        return queues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            val newTableName = renameFunction(queue)
            SchemaGenerator.generateRenameTableSchema(queue, existingTable, newTableName)
        }
    }

    /**
     * Generate all statements needed to rename queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateRenameStatements(
        dataSource: DataSource,
        vararg queues: Queue<*>,
        renameFunction: (Queue<*>) -> String
    ): Map<Queue<*>, Schema> {
        return generateRenameStatements(dataSource, queues.toList(), renameFunction)
    }

    /**
     * Rename queue tables
     *
     * Every Kolbasa queue has its own real table in the database. This method renames the queue table based on the
     * existing database state and the queue's real table name, since the table name differs from the queue name.
     *
     * This is a convenient method that allows you to rename the table in the database to a new name generated by
     * the [renameFunction].
     */
    @JvmStatic
    fun renameQueues(dataSource: DataSource, queues: List<Queue<*>>, renameFunction: (Queue<*>) -> String): SchemaResult {
        val mergedSchema = generateRenameStatements(dataSource, queues, renameFunction).values.merge()
        return executeSchemaStatements(dataSource, mergedSchema)
    }

    /**
     * Rename queue tables
     *
     * See [renameQueues] for more details
     */
    @JvmStatic
    fun renameQueues(dataSource: DataSource, vararg queues: Queue<*>, renameFunction: (Queue<*>) -> String): SchemaResult {
        return renameQueues(dataSource, queues.toList(), renameFunction)
    }


    // ----------------------------------------------------------------------------------------
    // Delete functions
    /**
     * Generate all statements needed to rename queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateDeleteStatements(dataSource: DataSource, queues: List<Queue<*>>): Map<Queue<*>, Schema> {
        val existingTables = SchemaExtractor.extractRawSchema(dataSource, queues.map { it.dbTableName }.toSet())

        return queues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            SchemaGenerator.generateDropTableSchema(queue, existingTable)
        }
    }

    /**
     * Generate all statements needed to delete queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateDeleteStatements(dataSource: DataSource, vararg queues: Queue<*>): Map<Queue<*>, Schema> {
        return generateDeleteStatements(dataSource, queues.toList())
    }

    /**
     * Rename queue tables
     *
     * Every Kolbasa queue has its own real table in the database. This method drops the queue table based on the
     * existing database state and the queue's real table name, since the table name differs from the queue name.
     *
     * This is a convenient method that allows you to drop the table in the database
     */
    @JvmStatic
    fun deleteQueues(dataSource: DataSource, queues: List<Queue<*>>): SchemaResult {
        val mergedSchema = generateDeleteStatements(dataSource, queues).values.merge()
        return executeSchemaStatements(dataSource, mergedSchema)
    }

    /**
     * Delete queue tables
     *
     * See [deleteQueues] for more details
     */
    @JvmStatic
    fun deleteQueues(dataSource: DataSource, vararg queues: Queue<*>): SchemaResult {
        return deleteQueues(dataSource, queues.toList())
    }


    @JvmStatic
    fun executeSchemaStatements(dataSource: DataSource, schema: Schema): SchemaResult {
        if (schema.isEmpty) {
            // nothing to execute
            return SchemaResult(schema, 0, emptyList(), emptyList())
        }

        // separate transaction for each statement
        return dataSource.useConnectionWithAutocommit { connection ->
            connection.createStatement().use { statement ->

                // Execute table statements
                // Try to execute them in chunks to reduce number of round-trips to the database
                val failedTableStatements = mutableListOf<FailedStatement>()
                schema.tableStatements.chunked(DEFAULT_CHUNK_SIZE).forEach { statementsChunk ->
                    try {
                        val combined = statementsChunk.joinToString(separator = ";")
                        statement.execute(combined)
                    } catch (_: Exception) {
                        // If we failed to execute the chunk, try to execute statements one by one
                        statementsChunk.forEach { sql ->
                            try {
                                statement.execute(sql)
                            } catch (e: Exception) {
                                failedTableStatements += FailedStatement(sql, e)
                            }
                        }
                    }
                }

                // Execute index statements
                // Can't batch them because almost all Kolbasa indexes use CREATE INDEX CONCURRENTLY which
                // cannot be executed inside a transaction block
                val failedIndexStatements = mutableListOf<FailedStatement>()
                schema.indexStatements.forEach { sql ->
                    try {
                        statement.execute(sql)
                    } catch (e: Exception) {
                        failedIndexStatements += FailedStatement(sql, e)
                    }
                }

                SchemaResult(
                    schema = schema,
                    failedStatements = failedTableStatements.size + failedIndexStatements.size,
                    failedTableStatements = failedTableStatements,
                    failedIndexStatements = failedIndexStatements
                )
            }
        }
    }

    private const val DEFAULT_CHUNK_SIZE = 25

}

data class SchemaResult(
    val schema: Schema,
    val failedStatements: Int,
    val failedTableStatements: List<FailedStatement>,
    val failedIndexStatements: List<FailedStatement>
) {
    init {
        check(failedTableStatements.size + failedIndexStatements.size == failedStatements) {
            "Inconsistent schema result: failedStatements=$failedStatements, " +
                "failedTableStatements=${failedTableStatements.size}, " +
                "failedIndexStatements=${failedIndexStatements.size}"
        }
    }
}

data class FailedStatement(val statement: String, val error: Exception)
